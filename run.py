#!/usr/bin/env python3
"""Command line.

  python run.py numbers                       rough capacity numbers
  python run.py models                        list switchable models
  python run.py gen --n 400                   dummy invoices + vendor master + POs
  python run.py run --model sim-fast          one run (all stage pools in parallel)
  python run.py eval --chaos heavy            every simulated model + naive-poster ablation, report
  python run.py scale --workers 1,2,4,8,16,32 throughput vs extraction workers
Model can also come from INVOICE_MODEL / INVOICE_FALLBACK.
"""
from __future__ import annotations

import argparse
import json
import os

from app.config import RUNS_DIR, Chaos, PoolSizes, Settings


def _settings(a, model, naive=False) -> Settings:
    fb = None if a.fallback in ("none", model) else a.fallback
    return Settings(model=model, fallback=fb, chaos=Chaos.preset(a.chaos), seed=a.seed, rpm=a.rpm,
                    naive_poster=naive,
                    pools=PoolSizes(ingest=a.ingest, extraction=a.extraction, review=a.review, posting=a.posting))


def _one(s: Settings) -> dict:
    from app.eval.evaluate import evaluate
    from app.runtime import Runtime
    print(f"\n== {s.name} (fallback {s.fallback or 'none'}, pools {vars(s.pools)}) ==")
    rt = Runtime(s)
    rt.run()
    r = evaluate(rt.dir)
    sf, o = r["safety"], r["outcomes"]
    print(f"   double payments {sf['double_payments']}  lost {sf['lost_invoices']}  wrong auto-posts "
          f"{sf['unsafe_auto_posts']} | auto-post {o['auto_post_rate']:.1%}  review {o['review_rate']:.1%}"
          f" | ${r['cost']['usd_per_invoice']:.4f}/invoice | {r['throughput']['wall_s']}s wall")
    return r


def cmd_numbers(a):
    from app.eval.capacity import estimate, render
    print(render(estimate(a.per_day, a.month_end_x, a.review_rate)))


def cmd_models(a):
    from app.adapters.models import load_registry
    for name, s in load_registry().items():
        print(f"{name:14} {s['provider']:10} ${s['price_in_per_mtok']}/${s['price_out_per_mtok']} per Mtok  {s.get('description', '')}")


def cmd_gen(a):
    from app.sim.data_gen import generate
    print(json.dumps(generate(a.n, a.seed), indent=1))


def cmd_run(a):
    model = a.model or os.environ.get("INVOICE_MODEL", "sim-strong")
    a.fallback = a.fallback if a.fallback != "sim-fast" else os.environ.get("INVOICE_FALLBACK", a.fallback)
    print(json.dumps(_one(_settings(a, model)), indent=1, default=str))


def cmd_eval(a):
    from app.eval.report import write_report
    models = a.models.split(",")
    results = [_one(_settings(a, m)) for m in models]
    if a.ablation:
        results.append(_one(_settings(a, models[0], naive=True)))
    scaling = None
    if a.with_scaling:
        from app.eval.scaling import run_scaling
        print("\n== scaling ==")
        scaling = run_scaling(log=print)
    RUNS_DIR.mkdir(exist_ok=True)
    (RUNS_DIR / "comparison.json").write_text(json.dumps({"chaos": a.chaos, "results": results,
                                                          "scaling": scaling}, indent=1, default=str))
    print(f"\nwrote {write_report(results, a.chaos, RUNS_DIR / 'report.html', scaling)}")


def cmd_scale(a):
    from app.eval.scaling import run_scaling
    rows = run_scaling(tuple(int(x) for x in a.workers.split(",")), a.model, a.rpm, a.chaos)
    RUNS_DIR.mkdir(exist_ok=True)
    (RUNS_DIR / "scaling.json").write_text(json.dumps(rows, indent=1))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    n = sub.add_parser("numbers"); n.add_argument("--per-day", type=int, default=20000)
    n.add_argument("--month-end-x", type=float, default=3.0); n.add_argument("--review-rate", type=float, default=0.08)
    n.set_defaults(f=cmd_numbers)
    sub.add_parser("models").set_defaults(f=cmd_models)
    g = sub.add_parser("gen"); g.add_argument("--n", type=int, default=400); g.add_argument("--seed", type=int, default=7)
    g.set_defaults(f=cmd_gen)
    sc = sub.add_parser("scale"); sc.add_argument("--workers", default="1,2,4,8,16,32")
    sc.add_argument("--model", default="sim-strong"); sc.add_argument("--rpm", type=int, default=120)
    sc.add_argument("--chaos", default="default", choices=["none", "default", "heavy"]); sc.set_defaults(f=cmd_scale)
    for name, fn in (("run", cmd_run), ("eval", cmd_eval)):
        p = sub.add_parser(name)
        if name == "run":
            p.add_argument("--model")
        else:
            p.add_argument("--models", default="sim-strong,sim-fast,sim-weak,sim-flaky")
            p.add_argument("--no-ablation", dest="ablation", action="store_false")
            p.add_argument("--no-scaling", dest="with_scaling", action="store_false")
        p.add_argument("--fallback", default="sim-fast", help="secondary model, or 'none'")
        p.add_argument("--chaos", default="default", choices=["none", "default", "heavy"])
        p.add_argument("--seed", type=int, default=42)
        p.add_argument("--rpm", type=int, default=600, help="model requests/minute across all workers")
        p.add_argument("--ingest", type=int, default=3); p.add_argument("--extraction", type=int, default=8)
        p.add_argument("--review", type=int, default=4); p.add_argument("--posting", type=int, default=2)
        p.set_defaults(f=fn)
    a = ap.parse_args()
    a.f(a)


if __name__ == "__main__":
    main()
