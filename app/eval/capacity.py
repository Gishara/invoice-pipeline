"""Rough numbers. Every assumption is a parameter; change them and re-run."""
from __future__ import annotations


def estimate(per_day=20000, month_end_x=3.0, review_rate=0.08, busy_share=0.8, busy_hours=10,
             peak_x=2.0, tokens_in=5000, tokens_out=1000, p50_s=10, p99_s=40, review_s=90,
             pdf_kb=300, retry_overhead=0.10):
    out = {}
    for label, n in (("normal day", per_day), ("month-end day", per_day * month_end_x)):
        avg = n * busy_share / (busy_hours * 3600)
        peak = avg * peak_x
        out[label] = {
            "invoices/day": n,
            "avg rate (/s, busy window)": round(avg, 2),
            "peak rate (/s)": round(peak, 2),
            "LLM tokens/day (M)": round(n * (tokens_in + tokens_out) / 1e6),
            "provider TPM needed at peak (M)": round(peak * 60 * (tokens_in + tokens_out) * (1 + retry_overhead) / 1e6, 2),
            "provider RPM needed at peak": round(peak * 60 * (1 + retry_overhead)),
            "calls in flight p50..p99": f"{peak * p50_s:.0f}..{peak * p99_s:.0f}",
            "people in review (hours/day)": round(n * review_rate * review_s / 3600),
            "PDF storage/day (GB)": round(n * pdf_kb / 1e6, 1),
            "DB writes/s at peak (~10/invoice)": round(peak * 10),
        }
    return out


def render(est: dict) -> str:
    keys = list(next(iter(est.values())))
    w = max(map(len, keys)) + 2
    lines = [" " * w + "".join(f"{k:>18}" for k in est)]
    for k in keys:
        lines.append(f"{k:<{w}}" + "".join(f"{str(v[k]):>18}" for v in est.values()))
    return "\n".join(lines)
