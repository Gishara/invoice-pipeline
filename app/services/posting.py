"""Posting: outbox + idempotency key + look-up before any retry.

The only rule that keeps an at-least-once world from paying twice: once a post
MAY have reached the ERP, never send it again until the ERP says it is not there."""
from __future__ import annotations

import json

from app.domain.model import idempotency_key
from app.ports import (Conflict, CrashInjector, DuplicateBusinessKey, ErpClient, ErpError, ErpTimeout,
                       InvoiceRepository)


class PostingService:
    def __init__(self, repo: InvoiceRepository, erp: ErpClient, crash: CrashInjector, clock,
                 naive: bool = False, settle_s: float = 60.0, claim_lease_s: float = 120.0):
        self.repo, self.erp, self.crash, self.clock, self.naive = repo, erp, crash, clock, naive
        self.settle_s = settle_s  # give the ERP time to make a slow post visible before we look it up
        self.claim_lease_s = max(claim_lease_s, settle_s)

    def post(self, inv_id: int, actor: str = "poster") -> str:
        row = self.repo.get(inv_id)
        if row["state"] == "POSTED":
            return "POSTED"
        if row["state"] in ("VALIDATED", "APPROVED"):
            f = json.loads(row["fields_json"])
            key = idempotency_key(row["supplier_id"], row["invoice_no_norm"])
            payload = {"supplier_id": row["supplier_id"], "invoice_number": f["invoice_number"],
                       "invoice_date": f["invoice_date"], "currency": f["currency"], "total": f["total"],
                       "po_number": f.get("po_number"), "lines": f["lines"], "source_invoice_id": inv_id}
            try:
                self.repo.begin_posting(inv_id, {"VALIDATED", "APPROVED"}, key, payload)
            except DuplicateBusinessKey:
                self.repo.transition(inv_id, {"VALIDATED", "APPROVED"}, "DUPLICATE", actor,
                                     "business key already posting or posted (unique index)")
                return "DUPLICATE"
            row = self.repo.get(inv_id)
        if row["state"] not in ("POSTING", "POST_UNKNOWN"):
            return row["state"]
        ob = self.repo.outbox(inv_id)
        key, payload = ob["idempotency_key"], json.loads(ob["payload"])
        attempt = self.repo.outbox_claim(inv_id, self.claim_lease_s, self.clock.now())
        if attempt is None:
            return self.repo.get(inv_id)["state"]
        # Use the attempt number returned by the atomic claim, not the earlier read:
        # another worker may have completed an ERP call between those two operations.
        uncertain = attempt > 1 or row["state"] == "POST_UNKNOWN"
        if uncertain and not self.naive:
            found = self.erp.lookup(key)
            if found:
                return self._posted(inv_id, found, "found in ERP before retry")
        if row["state"] == "POST_UNKNOWN":
            self.repo.transition(inv_id, {"POST_UNKNOWN"}, "POSTING", actor,
                                 "naive blind retry" if self.naive else "confirmed absent in ERP")
        self.crash.maybe_crash("posting:before_erp_call")
        try:
            doc = self.erp.post(key, payload)
        except (ErpTimeout, ErpError) as e:
            self.repo.transition(inv_id, {"POSTING"}, "POST_UNKNOWN", actor, f"{type(e).__name__}: {e}",
                                 next_attempt_at=self.clock.now() + self.settle_s)
            return "POST_UNKNOWN"
        self.crash.maybe_crash("posting:after_erp_call")
        return self._posted(inv_id, doc, "posted")

    def _posted(self, inv_id, doc_id, why) -> str:
        self.repo.outbox_done(inv_id, doc_id)
        try:
            self.repo.transition(inv_id, {"POSTING", "POST_UNKNOWN"}, "POSTED", "poster", f"{why} {doc_id}")
        except Conflict:
            pass  # another worker recorded it first; the ERP still has exactly one document
        return "POSTED"
