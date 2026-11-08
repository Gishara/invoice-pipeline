"""Safety and design tests.  python -m pytest -q tests  (or: python -m tests.test_invariants)
Uses the dataset in data/ (python run.py gen first)."""
from __future__ import annotations

import threading
from pathlib import Path

from app.adapters.kafka_sim import GroupConsumer
from app.adapters.sqlite_repository import SqliteInvoiceRepository
from app.clock import ScaledClock
from app.config import RUNS_DIR, Chaos, PoolSizes, Settings
from app.domain.model import norm_invoice_no
from app.domain.rules import ArithmeticRule, RuleContext
from app.eval.evaluate import evaluate
from app.ports import Record
from app.runtime import GROUP, TOPIC, Runtime
from app.services.cache import LRUCache
from app.workers.pools import IngestWorker, StageWorker


def _run(name, chaos="heavy", **kw):
    s = Settings(chaos=Chaos.preset(chaos), seed=5, run_name=f"test-{name}", **kw)
    rt = Runtime(s, log=lambda *_: None)
    rt.run()
    return rt, evaluate(rt.dir)


# ------------------------------------------------------------------ end to end, in parallel
def test_parallel_heavy_chaos_never_double_pays_or_loses():
    _, r = _run("heavy", pools=PoolSizes(ingest=4, extraction=16, review=6, posting=4))
    assert r["completed"]
    assert r["safety"] == {"double_payments": 0, "lost_invoices": 0, "unsafe_auto_posts": 0, "wrong_posts_total": 0}
    assert not r["resilience"]["unexpected_errors"]


def test_full_topic_replay_creates_nothing_new():
    rt, _ = _run("replay", chaos="default")
    rows_before, erp_before = rt.repo.count_by_state(), len(rt.erp.db.execute("SELECT 1 FROM ap_invoice").fetchall())
    rt.broker.reset_group(GROUP)                                  # rewind the whole consumer group to offset 0
    stop = threading.Event()
    w = IngestWorker("replay", rt.clock, stop, rt.metrics, GroupConsumer(rt.broker, TOPIC, GROUP, 0, 1),
                     rt.repo, rt.objects, 0.0, 1)
    while w.step():
        pass
    assert rt.repo.count_by_state() == rows_before
    assert len(rt.erp.db.execute("SELECT 1 FROM ap_invoice").fetchall()) == erp_before


def test_rerunning_posting_on_posted_invoices_posts_nothing():
    rt, _ = _run("rerun", chaos="none")
    erp_before = len(rt.erp.db.execute("SELECT 1 FROM ap_invoice").fetchall())
    for row in rt.repo.query("SELECT id FROM invoice WHERE state='POSTED' LIMIT 30"):
        rt.g_post.invoke({"invoice_id": row["id"], "trace": []}, {"configurable": {"thread_id": f"again-{row['id']}"}})
    assert len(rt.erp.db.execute("SELECT 1 FROM ap_invoice").fetchall()) == erp_before


def test_naive_poster_double_pays():
    """The ablation: the look-up before retry is what prevents double payments."""
    _, r = _run("naive", naive_poster=True)
    assert r["safety"]["double_payments"] > 0


def test_flaky_provider_fails_over_and_finishes():
    _, r = _run("flaky", chaos="default", model="sim-flaky")
    assert r["completed"] and r["safety"]["lost_invoices"] == 0
    assert r["provider"]["failovers"] > 0


# ------------------------------------------------------------------ units
def test_concurrent_claims_never_hand_out_a_row_twice(tmp_path: Path = RUNS_DIR / "test-claims"):
    tmp_path.mkdir(parents=True, exist_ok=True)
    db = tmp_path / "claims.db"
    db.unlink(missing_ok=True)
    repo = SqliteInvoiceRepository(db, ScaledClock(1.0))
    for i in range(300):
        repo.insert_received(f"sha{i}", f"ref{i}", Record("t", 0, i, "k", b""))
    claimed, lock = [], threading.Lock()

    def grab(w):
        while (row := repo.claim(("RECEIVED",), f"w{w}", lease_s=1e6, now=0)) is not None:
            with lock:
                claimed.append(row["id"])
    threads = [threading.Thread(target=grab, args=(i,)) for i in range(16)]
    [t.start() for t in threads]; [t.join() for t in threads]
    assert sorted(claimed) == list(range(1, 301))


def test_invoice_number_normalisation():
    assert norm_invoice_no("INV-00123") == norm_invoice_no("inv 123") == norm_invoice_no("INV/0123") == "INV123"


def test_rules_are_pluggable():
    f = {"lines": [{"quantity": 2, "unit_price": 5.0, "line_total": 10.0}], "subtotal": 10.0, "tax": 0, "total": 11}
    from app.config import Policy
    assert ArithmeticRule().check(RuleContext(f, None, None, Policy())) == ["subtotal_plus_tax_ne_total"]


def test_lru_cache_counts_hits():
    c, calls = LRUCache(2), []
    for k in ["a", "a", "b", "a", "c", "b"]:
        c.get_or_load(k, lambda k=k: calls.append(k) or k)
    assert calls == ["a", "b", "c", "b"] and c.hits == 2


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn(); print("ok", name)


def test_copy_with_different_amount_waits_for_the_original_under_review():
    """Regression: the original went to review (model misread it); a re-sent copy with a
    different amount must not be auto-posted while a person still holds the original."""
    from app.services.validation import ValidationService

    class Repo:
        def earlier_from_same_sender(self, inv_id): return []
        def same_business_key(self, sid, no, ex): return [{"id": 1, "state": "NEEDS_REVIEW", "fields_json": "{}"}]

    class Ref:
        v = {"supplier_id": "V1", "name": "Acme", "tax_id": "T1", "currency": "EUR", "iban": "X",
             "invoice_no_pattern": "INV{n:03d}"}
        def vendor_by_tax_id(self, t): return self.v
        def vendor_by_name(self, n): return self.v
        def purchase_order(self, po): return {"po_number": "PO-1", "supplier_id": "V1",
                                              "lines": [{"sku": "S", "quantity": 9, "unit_price": 10.0}]}
        def is_po_number(self, s): return False
    from app.config import Policy
    f = {"supplier_name": "Acme", "supplier_tax_id": "T1", "invoice_number": "INV001", "invoice_date": "2026-11-01",
         "currency": "EUR", "po_number": "PO-1", "iban": "X", "subtotal": 10.0, "tax": 0.0, "total": 10.0,
         "lines": [{"sku": "S", "description": "d", "quantity": 1, "unit_price": 10.0, "line_total": 10.0}]}
    v = ValidationService(Repo(), Ref(), Policy()).evaluate(2, f, [])
    assert v.outcome == "review" and any("under_review" in r for r in v.reasons)


def test_concurrent_posting_attempts_reserve_the_outbox_once(tmp_path: Path):
    """A second worker must not call the ERP while the first attempt is still in flight."""
    import json

    from app.services.posting import PostingService

    class NoCrash:
        def maybe_crash(self, point): pass

    class BlockingErp:
        def __init__(self):
            self.entered, self.release = threading.Event(), threading.Event()
            self.calls = 0

        def post(self, external_ref, payload):
            self.calls += 1
            self.entered.set()
            assert self.release.wait(5)
            return "ERP-0000001"

        def lookup(self, external_ref): return None

    clock = ScaledClock(1.0)
    repo = SqliteInvoiceRepository(tmp_path / "posting.db", clock)
    fields = {"invoice_number": "INV-1", "invoice_date": "2026-11-01", "currency": "USD",
              "total": 10.0, "po_number": "PO-1", "lines": []}
    repo.insert_received("sha", "pdf", Record("topic", 0, 0, "sender", b"pdf"))
    repo.transition(1, {"RECEIVED"}, "VALIDATED", "test", supplier_id="V001",
                     invoice_no_norm="INV1", fields_json=json.dumps(fields))
    erp = BlockingErp()
    poster = PostingService(repo, erp, NoCrash(), clock)
    result, errors = [], []

    def first_attempt():
        try:
            result.append(poster.post(1))
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=first_attempt)
    worker.start()
    try:
        assert erp.entered.wait(5)
        assert poster.post(1) == "POSTING"
        assert erp.calls == 1
    finally:
        erp.release.set()
        worker.join(5)

    assert not worker.is_alive()
    assert not errors
    assert result == ["POSTED"]
    assert repo.get(1)["state"] == "POSTED"
    assert erp.calls == 1
