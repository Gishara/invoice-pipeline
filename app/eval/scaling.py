"""Scaling benchmark: the same invoices, all queued at once, with more and more
extraction workers. Throughput should rise roughly linearly until the provider
rate limit (requests/minute) caps it; past that point more workers add nothing
but queueing at the gateway. The invariants must hold at every pool size."""
from __future__ import annotations

from dataclasses import replace

from app.config import Chaos, PoolSizes, Settings
from app.eval.evaluate import evaluate
from app.runtime import Runtime


def run_scaling(worker_counts=(1, 2, 4, 8, 16, 32), model="sim-strong", rpm=120, chaos="default",
                log=print) -> list[dict]:
    rows = []
    for n in worker_counts:
        s = Settings(model=model, fallback="sim-fast", chaos=Chaos.preset(chaos), rpm=rpm,
                     pools=PoolSizes(ingest=3, extraction=n, review=8, posting=2),
                     review_think_s=10, time_scale=0.02, run_name=f"scale-{n:02d}")
        rt = Runtime(s, log=lambda *_: None)
        m = rt.run(batch=10_000, every_s=0)          # whole backlog at once, like a month-end spike
        r = evaluate(rt.dir)
        row = {"extraction_workers": n, "rpm_limit": rpm,
               "extraction_per_hour": m["extraction_per_simulated_hour"],
               "drain_minutes": m["extraction_drained_minutes"],
               "max_concurrent_calls": m["gateway"]["max_concurrent"],
               "throttle_wait_s": round(m["gateway"]["throttle_wait_s"]),
               "rate_limited": m["gateway"]["rate_limited"],
               "conflicts": r["resilience"]["conflicts_between_workers"],
               "double_payments": r["safety"]["double_payments"], "lost": r["safety"]["lost_invoices"],
               "wrong_auto_posts": r["safety"]["unsafe_auto_posts"], "wall_s": m["wall_s"]}
        rows.append(row)
        log(f"   {n:>2} workers: {row['extraction_per_hour']:>6}/h  drain {row['drain_minutes']:>5} min"
            f"  concurrent {row['max_concurrent_calls']:>2}  throttled {row['throttle_wait_s']:>5}s"
            f"  double-pay {row['double_payments']} lost {row['lost']}")
    return rows
