"""Scores one run against the ground truth. The two numbers that must be zero
are double_payments and lost_invoices; everything else is a trade-off."""
from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

from app.adapters.models import load_registry
from app.config import DATA_DIR, Policy
from app.domain.model import TERMINAL as TERMINAL_STATES, InvoiceExtraction, norm_invoice_no

AUTO_POST_LIMIT = Policy().auto_post_limit

FIELDS = ["supplier_name", "supplier_tax_id", "invoice_number", "invoice_date", "currency", "po_number", "total"]
DEFECT_RULE = {  # scenario -> words we expect in the review reasons
    "items_mismatch": "items_mismatch", "wrong_invoice_no": "invoice_number",
    "arithmetic_error": "ne_total", "unknown_supplier": "unknown_supplier",
    "bank_details_changed": "bank_details", "dup_conflict": "duplicate",
    "over_limit": "above_auto_post_limit",
}


def _same(a, b) -> bool:
    if isinstance(a, float) or isinstance(b, float):
        try:
            return abs(float(a) - float(b)) < 0.005
        except (TypeError, ValueError):
            return False
    return (a or None) == (b or None)


def evaluate(run_dir: Path) -> dict:
    truth = json.loads((DATA_DIR / "ground_truth.json").read_text())
    m = json.loads((run_dir / "run_metrics.json").read_text())
    db = sqlite3.connect(run_dir / "invoices.db"); db.row_factory = sqlite3.Row
    erp = sqlite3.connect(run_dir / "erp.db")
    rows = {r["doc_sha256"]: r for r in db.execute("SELECT * FROM invoice")}
    reviewed = {r[0] for r in db.execute("SELECT invoice_id FROM review_task")}
    primary = m["settings"]["model"]
    cache = {(r[0], r[1]): r[2] for r in db.execute("SELECT doc_sha256, model, raw FROM extraction_cache")}

    # ---- safety: double payments and lost invoices
    erp_docs = erp.execute("SELECT supplier_id, invoice_no, total, currency, invoice_date FROM ap_invoice").fetchall()
    per_key = Counter((s, norm_invoice_no(n)) for s, n, *_ in erp_docs)
    double_payments = sum(c - 1 for c in per_key.values() if c > 1)
    lost = [t["seq"] for t in truth if t["doc_sha256"] not in rows or rows[t["doc_sha256"]]["state"] not in TERMINAL_STATES]

    # ---- outcomes vs expectation (duplicate groups judged as a group)
    groups = defaultdict(list)
    for t in truth:
        groups[t["business_key"]].append(t)
    outcome_ok, by_scenario = 0, defaultdict(Counter)
    wrong_outcomes = []
    for t in truth:
        r = rows.get(t["doc_sha256"])
        final = r["state"] if r else "MISSING"
        auto = r is not None and r["id"] not in reviewed
        by_scenario[t["scenario"]][f"{final}{' (auto)' if auto and final == 'POSTED' else ''}"] += 1
        exp = t["expected_final"]
        g = groups[t["business_key"]]
        if t["scenario"] in ("clean", "dup_resend") and len(g) > 1 and not any(x["scenario"] == "dup_conflict" for x in g):
            finals = [rows[x["doc_sha256"]]["state"] for x in g if x["doc_sha256"] in rows]
            ok = finals.count("POSTED") == 1 and all(f in ("POSTED", "DUPLICATE") for f in finals)
        else:
            ok = final == exp
        outcome_ok += ok
        if not ok:
            wrong_outcomes.append({"seq": t["seq"], "scenario": t["scenario"], "expected": exp, "got": final,
                                   "reasons": json.loads(r["reasons_json"] or "[]") if r else []})

    # ---- what got posted, and was it right?
    posted = [(t, rows[t["doc_sha256"]]) for t in truth
              if t["doc_sha256"] in rows and rows[t["doc_sha256"]]["state"] == "POSTED"]
    auto_posted = [(t, r) for t, r in posted if r["id"] not in reviewed]
    wrong_posted = []
    for t, r in posted:
        f = json.loads(r["fields_json"])
        bad = [k for k in ("invoice_number", "invoice_date", "currency", "total") if not _same(f.get(k), t["fields"][k])]
        if t["expected_final"] not in ("POSTED", "DUPLICATE") or bad:
            wrong_posted.append({"seq": t["seq"], "scenario": t["scenario"], "auto": r["id"] not in reviewed, "fields": bad})
    unsafe_auto = [w for w in wrong_posted if w["auto"]]

    # ---- review load
    review_reasons = Counter()
    clean = [t for t in truth if t["expected_final"] in ("POSTED", "DUPLICATE") and t["scenario"] != "over_limit"]
    unnecessary = 0
    for t in clean:
        r = rows.get(t["doc_sha256"])
        if r is not None and r["id"] in reviewed:
            unnecessary += 1
            for reason in json.loads(r["reasons_json"] or "[]"):
                review_reasons[reason.split(":")[0].split(" (")[0]] += 1
    defects = [t for t in truth if t["scenario"] in DEFECT_RULE]
    caught = sum(1 for t in defects if t["doc_sha256"] in rows and rows[t["doc_sha256"]]["id"] in reviewed)
    caught_right_reason = sum(
        1 for t in defects if t["doc_sha256"] in rows and rows[t["doc_sha256"]]["id"] in reviewed
        and DEFECT_RULE[t["scenario"]] in (rows[t["doc_sha256"]]["reasons_json"] or ""))
    dups = [t for t in truth if t["scenario"] == "dup_resend"]
    dups_auto_closed = sum(1 for t in dups if t["doc_sha256"] in rows
                           and rows[t["doc_sha256"]]["state"] == "DUPLICATE"
                           and rows[t["doc_sha256"]]["id"] not in reviewed)

    # ---- extraction accuracy (the model's own first valid answer, before any human correction)
    field_hits, field_n, lines_ok, n_extracted = Counter(), 0, 0, 0
    for t in truth:
        raw = cache.get((t["doc_sha256"], f"{primary}#0")) or cache.get((t["doc_sha256"], f"{primary}#1"))
        if not raw:
            continue
        try:
            f = InvoiceExtraction.model_validate_json(raw[raw.index("{"): raw.rindex("}") + 1]).model_dump()
        except Exception:
            continue
        n_extracted += 1
        for k in FIELDS:
            field_hits[k] += _same(f.get(k), t["fields"].get(k))
        lines_ok += len(f["lines"]) == len(t["fields"]["lines"]) and all(
            _same(a["quantity"], b["quantity"]) and _same(a["line_total"], b["line_total"])
            for a, b in zip(f["lines"], t["fields"]["lines"]))
    field_acc = {k: round(field_hits[k] / n_extracted, 4) for k in FIELDS} if n_extracted else {}
    if n_extracted:
        field_acc["line_items"] = round(lines_ok / n_extracted, 4)

    n = len(truth)
    g, mm = m["gateway"], m["metrics"]
    review_rate = len([t for t in truth if t["doc_sha256"] in rows and rows[t["doc_sha256"]]["id"] in reviewed]) / n
    cost_per_inv = g["cost_usd"] / n
    res = {
        "model": primary, "naive_poster": m["settings"]["naive_poster"], "fallback": m["settings"]["fallback"],
        "description": load_registry()[primary].get("description", ""), "pools": m["settings"]["pools"],
        "completed": m["completed"],
        "invoices": n,
        "safety": {"double_payments": double_payments, "lost_invoices": len(lost),
                   "unsafe_auto_posts": len(unsafe_auto), "wrong_posts_total": len(wrong_posted)},
        "outcomes": {"correct_final_state": round(outcome_ok / n, 4), "auto_post_rate": round(len(auto_posted) / n, 4),
                     "review_rate": round(review_rate, 4),
                     "unnecessary_review_rate": round(unnecessary / max(1, len(clean)), 4),
                     "defect_catch_rate": round(caught / max(1, len(defects)), 4),
                     "defect_caught_for_right_reason": round(caught_right_reason / max(1, len(defects)), 4),
                     "duplicates_auto_closed": f"{dups_auto_closed}/{len(dups)}"},
        "extraction": {"valid_output_rate": round(n_extracted / n, 4), "field_accuracy": field_acc,
                       "repairs": m["extraction_repairs"]},
        "throughput": {"wall_s": m["wall_s"], "simulated_minutes": m["simulated_minutes"],
                       "invoices_per_simulated_hour": m["invoices_per_simulated_hour"],
                       "max_concurrent_model_calls": g["max_concurrent"], "throttle_wait_s": round(g["throttle_wait_s"])},
        "caches": {**m["caches"], "prompt_cache_tokens": g["tokens_cached"],
                   "prompt_cache_saving_usd": round(g["cost_without_prompt_cache_usd"] - g["cost_usd"], 4)},
        "unnecessary_review_reasons": dict(review_reasons.most_common(8)),
        "provider": {k: g[k] for k in ("calls", "ok", "timeouts", "rate_limited", "server_errors", "failovers",
                                       "unavailable", "by_model", "breaker_trips")},
        "cost": {"usd_total": round(g["cost_usd"], 4), "usd_per_invoice": round(cost_per_inv, 5),
                 "usd_per_normal_day_20k": round(cost_per_inv * 20000, 0),
                 "usd_per_month_end_day_60k": round(cost_per_inv * 60000, 0),
                 "tokens_per_invoice": round((g["tokens_in"] + g["tokens_out"]) / n),
                 "review_hours_per_normal_day": round(review_rate * 20000 * 90 / 3600, 0),
                 "review_hours_per_month_end_day": round(review_rate * 60000 * 90 / 3600, 0)},
        "resilience": {"kafka_producer_duplicates": m["producer"]["duplicates"],
                       "redeliveries_collapsed": mm.get("redeliveries_collapsed", 0),
                       "consumer_crashes": mm.get("consumer_crashes", 0), "dead_lettered": mm.get("dead_lettered", 0),
                       "worker_crashes": m["worker_crashes_by_point"],
                       "resumed_from_checkpoint": mm.get("extraction_resumed_from_checkpoint", 0)
                       + mm.get("posting_resumed_from_checkpoint", 0),
                       "conflicts_between_workers": mm.get("extraction_conflicts", 0) + mm.get("posting_conflicts", 0),
                       "erp": m["erp"], "unexpected_errors": m["unexpected_errors"]},
        "by_scenario": {k: dict(v) for k, v in sorted(by_scenario.items())},
        "wrong_outcomes": wrong_outcomes[:20], "wrong_posts": wrong_posted[:20], "lost": lost[:20],
        "auto_post_limit": AUTO_POST_LIMIT,
    }
    (run_dir / "evaluation.json").write_text(json.dumps(res, indent=1))
    return res
