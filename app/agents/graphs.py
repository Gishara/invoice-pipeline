"""LangGraph agents, one graph per stage.

The supervisor for an invoice's whole life is the state machine in Postgres: each
stage's pool claims rows in its states, runs its graph, and leaves the row in the
next stage's state. Inside the extraction stage, conditional edges route between
the agents. Splitting by stage lets each pool scale on its own bottleneck (model
rate limits, people, the ERP) while LangGraph checkpoints let any worker in a pool
resume a run another worker crashed in.

    extraction pool:  intake -> extraction_agent -> validation_agent -> END
    review pool:      open_review -> await_decision (interrupt) -> apply_decision -> END
    posting pool:     posting_agent -> END

Every node first reads the invoice row and acts only if the row is in the state it
expects, so a node re-run after a crash, or a duplicate run, does nothing extra."""
from __future__ import annotations

import json
import operator
import random
import threading
from typing import Annotated, Optional, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from app.config import Policy
from app.domain.model import norm_invoice_no
from app.ports import Conflict, InvoiceRepository, ProviderUnavailable, ReferenceData
from app.services.extraction import ExtractionService, PdfTextExtractor
from app.services.posting import PostingService
from app.services.validation import ValidationService


class SimulatedCrash(Exception):
    """The worker process died here."""


class RandomCrashInjector:
    def __init__(self, prob: float, seed: int):
        self.prob, self.rnd, self._lock, self.counts = prob, random.Random(seed), threading.Lock(), {}

    def maybe_crash(self, point: str) -> None:
        with self._lock:
            hit = self.rnd.random() < self.prob
            if hit:
                self.counts[point] = self.counts.get(point, 0) + 1
        if hit:
            raise SimulatedCrash(point)


class InvoiceState(TypedDict, total=False):
    invoice_id: int
    text: str
    fields: Optional[dict]
    outcome: str
    reasons: list[str]
    decision: Optional[dict]
    trace: Annotated[list[str], operator.add]


# ====================================================================== extraction stage
class ExtractionStage:
    def __init__(self, repo: InvoiceRepository, texts: PdfTextExtractor, extractor: ExtractionService,
                 validator: ValidationService, crash: RandomCrashInjector, clock, policy: Policy, seed: int):
        self.repo, self.texts, self.extractor, self.validator = repo, texts, extractor, validator
        self.crash, self.clock, self.policy = crash, clock, policy
        self.rnd = random.Random(seed)
        self._lock = threading.Lock()

    def intake(self, st: InvoiceState) -> dict:
        row = self.repo.get(st["invoice_id"])
        text = self.texts.text(row["doc_sha256"], row["pdf_ref"])
        if row["state"] == "RECEIVED":
            if len(text.strip()) < 40:
                self.repo.transition(row["id"], {"RECEIVED"}, "NEEDS_REVIEW", "intake", "unreadable_pdf",
                                     reasons_json=json.dumps(["unreadable_pdf"]))
                return {"text": text, "outcome": "review", "trace": ["intake:unreadable"]}
            self.repo.transition(row["id"], {"RECEIVED"}, "EXTRACTING", "intake", "",
                                 extraction_attempts=row["extraction_attempts"] + 1)
        return {"text": text, "trace": ["intake"]}

    def extraction_agent(self, st: InvoiceState) -> dict:
        row = self.repo.get(st["invoice_id"])
        if row["state"] != "EXTRACTING":
            f = json.loads(row["fields_json"]) if row["fields_json"] else None
            return {"fields": f, "outcome": "extracted" if row["state"] == "EXTRACTED" else "stop",
                    "trace": ["extract:skip"]}
        try:
            res = self.extractor.extract(row["doc_sha256"], st["text"], row["extraction_attempts"])
        except ProviderUnavailable as e:
            return self._provider_failed(row, str(e))
        if not res.from_cache:
            self.crash.maybe_crash("extraction:after_model_call")
        if res.fields is None:
            reasons = [f"model_output_invalid: {res.error}"]
            self.repo.transition(row["id"], {"EXTRACTING"}, "NEEDS_REVIEW", "extraction_agent", reasons[0],
                                 reasons_json=json.dumps(reasons), model_used=res.model)
            return {"outcome": "review", "reasons": reasons, "trace": ["extract:invalid_output"]}
        self.repo.transition(row["id"], {"EXTRACTING"}, "EXTRACTED", "extraction_agent", ",".join(res.ungrounded),
                             fields_json=json.dumps(res.fields), model_used=res.model,
                             reasons_json=json.dumps(res.ungrounded))
        return {"fields": res.fields, "outcome": "extracted", "trace": [f"extract:{res.model}"]}

    def _provider_failed(self, row, err: str) -> dict:
        if row["extraction_attempts"] >= self.policy.max_extraction_attempts:
            r = [f"extraction_failed_after_{row['extraction_attempts']}_attempts: {err}"]
            self.repo.transition(row["id"], {"EXTRACTING"}, "NEEDS_REVIEW", "extraction_agent", r[0],
                                 reasons_json=json.dumps(r))
            return {"outcome": "review", "trace": ["extract:gave_up"]}
        with self._lock:
            jitter = self.rnd.uniform(0.8, 1.2)
        delay = min(1800, 60 * 2 ** row["extraction_attempts"]) * jitter
        self.repo.transition(row["id"], {"EXTRACTING"}, "RECEIVED", "extraction_agent", f"provider: {err}",
                             next_attempt_at=self.clock.now() + delay)
        return {"outcome": "retry_later", "trace": ["extract:retry_later"]}

    def validation_agent(self, st: InvoiceState) -> dict:
        row = self.repo.get(st["invoice_id"])
        if row["state"] != "EXTRACTED":
            return {"outcome": "stop", "trace": [f"validate:skip_{row['state']}"]}
        f = json.loads(row["fields_json"])
        v = self.validator.evaluate(row["id"], f, json.loads(row["reasons_json"] or "[]"))
        if v.outcome == "defer":  # stay EXTRACTED; the pool re-runs only validation shortly
            self.repo.park(row["id"], until=self.clock.now() + 10)
            return {"outcome": "defer", "trace": ["validate:defer"]}
        to = {"auto_post": "VALIDATED", "review": "NEEDS_REVIEW", "duplicate": "DUPLICATE"}[v.outcome]
        self.repo.transition(row["id"], {"EXTRACTED"}, to, "validation_agent", "; ".join(v.reasons),
                             supplier_id=v.supplier_id, invoice_no_norm=v.invoice_no_norm, total=f["total"],
                             currency=f["currency"], reasons_json=json.dumps(v.reasons))
        return {"outcome": v.outcome, "reasons": v.reasons, "trace": [f"validate:{v.outcome}"]}

    def build(self, checkpointer):
        g = StateGraph(InvoiceState)
        g.add_node("intake", self.intake)
        g.add_node("extraction_agent", self.extraction_agent)
        g.add_node("validation_agent", self.validation_agent)
        g.add_edge(START, "intake")
        g.add_conditional_edges("intake", lambda s: END if s.get("outcome") == "review" else "extraction_agent")
        g.add_conditional_edges("extraction_agent",
                                lambda s: "validation_agent" if s.get("outcome") == "extracted" else END)
        g.add_edge("validation_agent", END)
        return g.compile(checkpointer=checkpointer)


# ====================================================================== review stage
class ReviewStage:
    def __init__(self, repo: InvoiceRepository, ref: ReferenceData):
        self.repo, self.ref = repo, ref

    def open_review(self, st: InvoiceState) -> dict:
        row = self.repo.get(st["invoice_id"])
        if row["state"] == "NEEDS_REVIEW":
            self.repo.open_review(row["id"], json.loads(row["reasons_json"] or "[]"),
                                  json.loads(row["fields_json"]) if row["fields_json"] else None)
        return {"trace": ["review:opened"]}

    def await_decision(self, st: InvoiceState) -> dict:
        # the graph pauses here; the checkpoint holds it until a person answers
        return {"decision": interrupt({"invoice_id": st["invoice_id"]}), "trace": ["review:answered"]}

    def apply_decision(self, st: InvoiceState) -> dict:
        row, d = self.repo.get(st["invoice_id"]), st["decision"]
        if row["state"] == "NEEDS_REVIEW":
            who = f"human:{d.get('reviewer', 'clerk')}"
            if d["decision"] == "approve":
                f = d["fields"]
                v = self.ref.vendor_by_tax_id(f["supplier_tax_id"])
                self.repo.transition(row["id"], {"NEEDS_REVIEW"}, "APPROVED", who, "approved",
                                     fields_json=json.dumps(f), supplier_id=v["supplier_id"] if v else None,
                                     invoice_no_norm=norm_invoice_no(f["invoice_number"]), total=f["total"],
                                     currency=f["currency"], next_attempt_at=0)  # hand to the posting pool
            else:
                self.repo.transition(row["id"], {"NEEDS_REVIEW"},
                                     "DUPLICATE" if d["decision"] == "duplicate" else "REJECTED", who, d["decision"])
        self.repo.close_review(row["id"], d["decision"], d.get("reviewer", "clerk"))
        return {"outcome": d["decision"], "trace": [f"review:{d['decision']}"]}

    def build(self, checkpointer):
        g = StateGraph(InvoiceState)
        g.add_node("open_review", self.open_review)
        g.add_node("await_decision", self.await_decision)
        g.add_node("apply_decision", self.apply_decision)
        g.add_edge(START, "open_review")
        g.add_edge("open_review", "await_decision")
        g.add_edge("await_decision", "apply_decision")
        g.add_edge("apply_decision", END)
        return g.compile(checkpointer=checkpointer)


# ====================================================================== posting stage
class PostingStage:
    def __init__(self, poster: PostingService):
        self.poster = poster

    def posting_agent(self, st: InvoiceState) -> dict:
        try:
            return {"outcome": self.poster.post(st["invoice_id"]), "trace": ["post"]}
        except Conflict:
            return {"outcome": "stop", "trace": ["post:conflict"]}

    def build(self, checkpointer):
        g = StateGraph(InvoiceState)
        g.add_node("posting_agent", self.posting_agent)
        g.add_edge(START, "posting_agent")
        g.add_edge("posting_agent", END)
        return g.compile(checkpointer=checkpointer)
