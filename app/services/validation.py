"""Validation service: business rules plus the two checks that need the database.

  * DuplicateGuard   same supplier + invoice number already on its way to the ERP
  * SenderOrdering   an earlier invoice from the same sender is still being extracted

Ordering matters once extraction runs in parallel. Kafka keeps one sender's
invoices in order on one partition, but eight extraction workers finish in any
order. Without it, a re-sent copy with a different amount could validate before
the original and get paid instead. So validation (cheap) is serialised per sender,
while extraction (expensive) stays fully parallel."""
from __future__ import annotations

import json
from dataclasses import dataclass

from app.config import Policy
from app.domain.model import norm_invoice_no
from app.domain.rules import Rule, RuleContext, default_rules, match_supplier
from app.ports import InvoiceRepository, ReferenceData

IN_FLIGHT_TO_ERP = {"POSTED", "POSTING", "POST_UNKNOWN", "VALIDATED", "APPROVED"}


@dataclass
class Verdict:
    outcome: str                # auto_post | review | duplicate | defer
    reasons: list[str]
    supplier_id: str | None
    invoice_no_norm: str | None


class ValidationService:
    def __init__(self, repo: InvoiceRepository, ref: ReferenceData, policy: Policy, rules: list[Rule] | None = None):
        self.repo, self.ref, self.policy = repo, ref, policy
        self.rules = rules if rules is not None else default_rules()

    def evaluate(self, inv_id: int, fields: dict, prior_reasons: list[str]) -> Verdict:
        if self._earlier_sender_invoice_pending(inv_id, norm_invoice_no(fields.get("invoice_number")) or None):
            return Verdict("defer", ["earlier invoice from this sender still in progress"], None, None)
        vendor, reasons = match_supplier(fields, self.ref)
        reasons = list(prior_reasons) + reasons
        ctx = RuleContext(fields, vendor, self.ref, self.policy)
        for rule in self.rules:
            reasons += rule.check(ctx)
        sid = vendor["supplier_id"] if vendor else None
        inv_norm = norm_invoice_no(fields.get("invoice_number")) or None
        outcome = "review" if reasons else "auto_post"
        if sid and inv_norm:
            kind, other_id = self._duplicate(inv_id, sid, inv_norm, fields)
            if kind == "same":
                return Verdict("duplicate", [f"duplicate_of_invoice_{other_id}"], sid, inv_norm)
            if kind == "conflict":
                outcome, reasons = "review", reasons + [f"possible_duplicate_conflict_with_invoice_{other_id}"]
            if kind == "under_review":  # a person is looking at the same invoice number right now
                outcome, reasons = "review", reasons + [f"same_invoice_number_under_review_{other_id}"]
        return Verdict(outcome, reasons, sid, inv_norm)

    def _earlier_sender_invoice_pending(self, inv_id: int, inv_norm: str | None) -> bool:
        """Wait only when an earlier invoice from the same sender could be the same invoice:
        it is not extracted yet (number unknown), or it is extracted with the same number."""
        for r in self.repo.earlier_from_same_sender(inv_id):
            if r["extraction_attempts"] >= self.policy.max_extraction_attempts:
                continue  # it is about to go to a person; don't block behind it
            if r["state"] != "EXTRACTED":
                return True
            other = json.loads(r["fields_json"] or "{}").get("invoice_number")
            if inv_norm and norm_invoice_no(other) == inv_norm:
                return True
        return False

    def _duplicate(self, inv_id, sid, inv_norm, f) -> tuple[str | None, int | None]:
        for other in self.repo.same_business_key(sid, inv_norm, inv_id):
            if other["state"] == "NEEDS_REVIEW":
                return "under_review", other["id"]
            if other["state"] not in IN_FLIGHT_TO_ERP:
                continue
            of = json.loads(other["fields_json"] or "{}")
            same = abs(of.get("total", -1) - f["total"]) <= self.policy.money_tolerance \
                and of.get("invoice_date") == f["invoice_date"]
            return ("same" if same else "conflict"), other["id"]
        return None, None
