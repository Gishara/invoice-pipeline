"""Renders runs/report.html from the evaluation results (self-contained page)."""
from __future__ import annotations

import html
import json
from pathlib import Path

from app.eval.capacity import estimate

CSS = """
/* Layout: one reading column; safety verdicts first, then the trade-off table, then detail per model. */
:root{
  --paper:#f6f7f9; --sheet:#ffffff; --ink:#18202b; --muted:#5b6675; --rule:#dde2e9;
  --accent:#1f4fa3; --accent-soft:#e6edf9; --good:#1d7a4a; --good-soft:#e3f3ea;
  --bad:#b3261e; --bad-soft:#fbe7e5; --warn:#8a5a00; --warn-soft:#fbf0d9;
  --display:"IBM Plex Sans Condensed","Arial Narrow",system-ui,sans-serif;
  --body:"IBM Plex Sans",system-ui,-apple-system,"Segoe UI",sans-serif;
  --mono:"IBM Plex Mono",ui-monospace,"SFMono-Regular",Menlo,monospace;
}
@media (prefers-color-scheme: dark){:root:not([data-theme="light"]){
  --paper:#11151b; --sheet:#181d25; --ink:#e7ebf1; --muted:#9aa5b4; --rule:#2a313c;
  --accent:#8fb2f0; --accent-soft:#1d2a40; --good:#6fcf97; --good-soft:#163024;
  --bad:#f28b82; --bad-soft:#3a1d1b; --warn:#e8bd62; --warn-soft:#3a2d12; color-scheme:dark}}
:root[data-theme="dark"]{
  --paper:#11151b; --sheet:#181d25; --ink:#e7ebf1; --muted:#9aa5b4; --rule:#2a313c;
  --accent:#8fb2f0; --accent-soft:#1d2a40; --good:#6fcf97; --good-soft:#163024;
  --bad:#f28b82; --bad-soft:#3a1d1b; --warn:#e8bd62; --warn-soft:#3a2d12; color-scheme:dark}
body{background:var(--paper);color:var(--ink);font:15px/1.55 var(--body);
  padding-left:max(16px,env(safe-area-inset-left,0px));padding-right:max(16px,env(safe-area-inset-right,0px));padding-block:32px 56px}
main{max-width:1040px;margin:0 auto;display:flex;flex-direction:column;gap:36px}
h1,h2,h3{font-family:var(--display);text-wrap:balance;margin:0;letter-spacing:.005em}
h1{font-size:34px;line-height:1.1;font-weight:600}
h2{font-size:22px;font-weight:600}
h3{font-size:16px;font-weight:600}
p{margin:0;max-width:68ch}
.lede{color:var(--muted)}
.eyebrow{font:500 12px/1 var(--mono);text-transform:uppercase;letter-spacing:.08em;color:var(--accent)}
header{display:flex;flex-direction:column;gap:10px}
.meta{display:flex;flex-wrap:wrap;gap:8px 18px;font:13px var(--mono);color:var(--muted)}
section{display:flex;flex-direction:column;gap:14px}
.scroll{overflow-x:auto;border:1px solid var(--rule);border-radius:6px;background:var(--sheet)}
table{border-collapse:collapse;width:100%;font-size:14px}
th,td{padding:9px 12px;text-align:left;border-bottom:1px solid var(--rule);white-space:nowrap}
th{font:600 12px var(--mono);text-transform:uppercase;letter-spacing:.05em;color:var(--muted);background:var(--sheet)}
tr:last-child td{border-bottom:0}
td.n{text-align:right;font-family:var(--mono);font-variant-numeric:tabular-nums}
th.n{text-align:right}
.model{font-weight:600}
.sub{display:block;font-size:12px;color:var(--muted);font-weight:400;white-space:normal;max-width:30ch}
.pill{display:inline-block;padding:2px 9px;border-radius:99px;font:600 12px var(--mono)}
.ok{background:var(--good-soft);color:var(--good)}
.bad{background:var(--bad-soft);color:var(--bad)}
.warn{background:var(--warn-soft);color:var(--warn)}
.verdicts{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px}
.verdict{background:var(--sheet);border:1px solid var(--rule);border-radius:6px;padding:14px 16px;display:flex;flex-direction:column;gap:8px}
.verdict.fail{border-color:var(--bad)}
.verdict .row{display:flex;justify-content:space-between;gap:8px;font-size:13px}
.bars{display:flex;flex-direction:column;gap:10px;background:var(--sheet);border:1px solid var(--rule);border-radius:6px;padding:16px}
.bar{display:grid;grid-template-columns:150px 1fr 70px;gap:12px;align-items:center;font-size:13px}
.track{height:14px;background:var(--accent-soft);border-radius:3px;position:relative;overflow:hidden}
.fill{position:absolute;inset:0 auto 0 0;background:var(--accent);border-radius:3px}
.fill.alt{background:var(--warn)}
.bar .v{font-family:var(--mono);font-variant-numeric:tabular-nums;text-align:right}
.tabs{display:flex;flex-wrap:wrap;gap:6px}
.tabs button{font:500 13px var(--mono);padding:6px 12px;border-radius:99px;border:1px solid var(--rule);background:var(--sheet);color:var(--ink);cursor:pointer}
.tabs button[aria-selected="true"]{background:var(--accent);border-color:var(--accent);color:var(--sheet)}
.tabs button:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px}
.grid2>*{min-width:0}
.note{font-size:13px;color:var(--muted)}
code{font:13px var(--mono);background:var(--accent-soft);padding:1px 5px;border-radius:4px}
pre{font:13px/1.6 var(--mono);background:var(--sheet);border:1px solid var(--rule);border-radius:6px;padding:14px;overflow-x:auto;margin:0}
@media (max-width:560px){.bar{grid-template-columns:96px 1fr 56px}h1{font-size:28px}}
@media (prefers-reduced-motion:no-preference){.fill{transition:width .4s ease}}
"""


def e(x) -> str:
    return html.escape(str(x))


def pct(x) -> str:
    return f"{x * 100:.1f}%"


def label(r) -> str:
    return r["model"] + (" · naive poster" if r.get("naive_poster") else "")


def write_report(results: list[dict], chaos: str, out: Path, scaling: list[dict] | None = None) -> Path:
    n = results[0]["invoices"]
    head = (f'<title>Invoice Pipeline Eval</title><meta name="claude:viewport-fit-sides" content="cover">'
            '<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
            '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600'
            '&family=IBM+Plex+Sans+Condensed:wght@500;600&family=IBM+Plex+Sans:wght@400;600&display=swap">'
            f"<style>{CSS}</style>")

    # --- safety verdicts
    cards = []
    for r in results:
        s = r["safety"]
        fail = s["double_payments"] or s["lost_invoices"] or s["unsafe_auto_posts"]
        rows = "".join(
            f'<div class="row"><span>{k}</span><span class="pill {"bad" if v else "ok"}">{v}</span></div>'
            for k, v in (("Double payments", s["double_payments"]), ("Lost invoices", s["lost_invoices"]),
                         ("Wrong auto-posts", s["unsafe_auto_posts"])))
        cards.append(f'<div class="verdict{" fail" if fail else ""}"><h3>{e(label(r))}</h3>{rows}</div>')

    # --- trade-off table
    trs = []
    for r in results:
        o, c, x = r["outcomes"], r["cost"], r["extraction"]
        trs.append(
            f'<tr><td class="model">{e(label(r))}<span class="sub">{e(r["description"])}</span></td>'
            f'<td class="n">{pct(o["auto_post_rate"])}</td><td class="n">{pct(o["review_rate"])}</td>'
            f'<td class="n">{pct(o["unnecessary_review_rate"])}</td><td class="n">{pct(o["defect_catch_rate"])}</td>'
            f'<td class="n">{pct(x["valid_output_rate"])}</td><td class="n">{pct(o["correct_final_state"])}</td>'
            f'<td class="n">${c["usd_per_invoice"]:.4f}</td><td class="n">${c["usd_per_month_end_day_60k"]:,.0f}</td>'
            f'<td class="n">{c["review_hours_per_month_end_day"]:,.0f} h</td></tr>')
    table = ('<div class="scroll"><table><thead><tr><th>Model</th><th class="n">Auto-posted</th><th class="n">To review</th>'
             '<th class="n">Clean sent to review</th><th class="n">Defects caught</th><th class="n">Valid JSON</th>'
             '<th class="n">Right outcome</th><th class="n">$/invoice</th><th class="n">LLM $ month-end day</th>'
             '<th class="n">Review month-end day</th></tr></thead><tbody>' + "".join(trs) + "</tbody></table></div>")

    # --- bars: review hours vs model cost at month-end (same scale within each chart)
    def bars(key_fn, fmt, cls=""):
        vals = [key_fn(r) for r in results]
        top = max(vals) or 1
        return "".join(f'<div class="bar"><span>{e(label(r))}</span><div class="track"><div class="fill {cls}" '
                       f'style="width:{v / top * 100:.1f}%"></div></div><span class="v">{fmt(v)}</span></div>'
                       for r, v in zip(results, vals))
    chart_review = bars(lambda r: r["cost"]["review_hours_per_month_end_day"], lambda v: f"{v:,.0f} h", "alt")
    chart_cost = bars(lambda r: r["cost"]["usd_per_month_end_day_60k"], lambda v: f"${v:,.0f}")

    # --- per-model detail panels
    tabs, panels = [], []
    for i, r in enumerate(results):
        pid = f"m{i}"
        tabs.append(f'<button role="tab" id="t-{pid}" aria-controls="{pid}" aria-selected="{str(i == 0).lower()}">{e(label(r))}</button>')
        fa = r["extraction"]["field_accuracy"]
        fields = "".join(f'<tr><td>{e(k.replace("_", " "))}</td><td class="n">{pct(v)}</td></tr>' for k, v in fa.items())
        scen = "".join(f'<tr><td>{e(k.replace("_", " "))}</td><td>{e(", ".join(f"{s} {c}" for s, c in v.items()))}</td></tr>'
                       for k, v in r["by_scenario"].items())
        reasons = "".join(f'<tr><td>{e(k.replace("_", " "))}</td><td class="n">{v}</td></tr>'
                          for k, v in r["unnecessary_review_reasons"].items()) or '<tr><td colspan="2">None</td></tr>'
        res, pv, ca, th = r["resilience"], r["provider"], r["caches"], r["throughput"]
        crashes = sum(res["worker_crashes"].values())
        resil = "".join(f'<tr><td>{e(k)}</td><td class="n">{v}</td></tr>' for k, v in (
            ("Producer duplicates written to the topic", res["kafka_producer_duplicates"]),
            ("Redeliveries collapsed at ingest", res["redeliveries_collapsed"]),
            ("Consumer crashes before offset commit", res["consumer_crashes"]),
            ("Poison messages kept in the dead-letter table", res["dead_lettered"]),
            ("Worker crashes inside a graph", crashes),
            ("Runs resumed from checkpoint by another worker", res["resumed_from_checkpoint"]),
            ("Workers that lost a race and backed off", res["conflicts_between_workers"]),
            ("ERP timeouts after the post landed", res["erp"]["timeouts_after_commit"]),
            ("ERP errors before the post landed", res["erp"]["errors_before_commit"]),
            ("ERP look-ups before a retry", res["erp"]["lookups"]),
            ("Model calls / timeouts / 429s / 5xx", f'{pv["calls"]} / {pv["timeouts"]} / {pv["rate_limited"]} / {pv["server_errors"]}'),
            ("Failovers to the fallback model", pv["failovers"]),
            ("Circuit-breaker trips", ", ".join(f"{k} {v}" for k, v in pv["breaker_trips"].items())),
            ("Most model calls in flight at once", th["max_concurrent_model_calls"]),
        ))
        caches = "".join(f'<tr><td>{e(k)}</td><td class="n">{v}</td></tr>' for k, v in (
            ("PDF text cache hit rate", pct(ca["pdf_text"]["hit_rate"])),
            ("Reference data cache hit rate", pct(ca["reference_data"]["hit_rate"])),
            ("Reference data reads that reached the source", ca["reference_data"]["source_reads"]),
            ("Extraction results reused instead of re-calling the model", ca["extraction_results_reused"]),
            ("Prompt tokens served from the provider cache", f'{ca["prompt_cache_tokens"]:,}'),
            ("Model spend saved by prompt caching", f'${ca["prompt_cache_saving_usd"]:.4f}'),
        ))
        wrong = r["wrong_posts"]
        wrong_html = ("" if not wrong else
                      '<p class="note">Wrong posts: ' + e("; ".join(f'#{w["seq"]} {w["scenario"]} {w["fields"] or ""}' for w in wrong[:8])) + "</p>")
        panels.append(f'''<div role="tabpanel" id="{pid}" aria-labelledby="t-{pid}"{"" if i == 0 else " hidden"}>
<div class="grid2">
 <div class="scroll"><table><thead><tr><th>Extracted field</th><th class="n">Correct</th></tr></thead><tbody>{fields}</tbody></table></div>
 <div class="scroll"><table><thead><tr><th>Why clean invoices went to review</th><th class="n">Count</th></tr></thead><tbody>{reasons}</tbody></table></div>
 <div class="scroll"><table><thead><tr><th>Failure injected or handled</th><th class="n">Count</th></tr></thead><tbody>{resil}</tbody></table></div>
 <div class="scroll"><table><thead><tr><th>Cache</th><th class="n">Effect</th></tr></thead><tbody>{caches}</tbody></table></div>
 <div class="scroll"><table><thead><tr><th>Scenario</th><th>Final state</th></tr></thead><tbody>{scen}</tbody></table></div>
</div>{wrong_html}</div>''')

    # --- scaling benchmark
    scaling_html = ""
    if scaling:
        top = max(x["extraction_per_hour"] for x in scaling) or 1
        sbars = "".join(f'<div class="bar"><span>{x["extraction_workers"]} workers</span><div class="track"><div class="fill" '
                        f'style="width:{x["extraction_per_hour"] / top * 100:.1f}%"></div></div>'
                        f'<span class="v">{x["extraction_per_hour"]:,}/h</span></div>' for x in scaling)
        srows = "".join(f'<tr><td class="n">{x["extraction_workers"]}</td><td class="n">{x["extraction_per_hour"]:,}</td>'
                        f'<td class="n">{x["drain_minutes"]}</td><td class="n">{x["max_concurrent_calls"]}</td>'
                        f'<td class="n">{x["throttle_wait_s"]:,}</td><td class="n">{x["conflicts"]}</td>'
                        f'<td><span class="pill {"bad" if x["double_payments"] or x["lost"] else "ok"}">'
                        f'{x["double_payments"]} / {x["lost"]}</span></td></tr>' for x in scaling)
        scaling_html = f"""<section>
 <h2>Scaling the extraction pool</h2>
 <p>All {n} invoices queued at once, like a month-end spike, with more extraction workers each run and a provider
 limit of {scaling[0]["rpm_limit"]} requests a minute shared by every worker. Throughput climbs with workers until
 that shared limit starts holding calls back; after that, more workers only wait at the gateway.</p>
 <div class="bars"><h3>Invoices extracted per hour</h3>{sbars}</div>
 <div class="scroll"><table><thead><tr><th class="n">Workers</th><th class="n">Per hour</th><th class="n">Backlog drained (min)</th>
 <th class="n">Calls in flight</th><th class="n">Throttled (s)</th><th class="n">Lost races</th><th>Double pay / lost</th></tr></thead>
 <tbody>{srows}</tbody></table></div>
</section>"""

    # --- rough numbers using the best non-naive model's measured review rate
    best = min((r for r in results if not r.get("naive_poster")), key=lambda r: r["outcomes"]["review_rate"])
    est = estimate(review_rate=best["outcomes"]["review_rate"])
    keys = list(next(iter(est.values())))
    num_rows = "".join(f'<tr><td>{e(k)}</td>' + "".join(f'<td class="n">{e(v[k])}</td>' for v in est.values()) + "</tr>" for k in keys)

    body = f"""
<main>
<header>
 <span class="eyebrow">Accounts payable · agent evaluation</span>
 <h1>Invoice Pipeline Eval</h1>
 <p class="lede">The same {n} dummy supplier invoices went through the LangGraph pipeline once per model, on a simulated
 at-least-once Kafka topic, with every stage running as its own parallel worker pool and crashes, provider errors
 and ERP timeouts injected ({e(chaos)} chaos preset).
 Ground truth decides every score.</p>
 <div class="meta"><span>{n} invoices</span><span>chaos: {e(chaos)}</span><span>auto-post limit {results[0]["auto_post_limit"]:,.0f}</span><span>models: {e(", ".join(label(r) for r in results))}</span></div>
</header>

<section>
 <h2>Money safety</h2>
 <p>These must all be zero. The naive-poster run is the same pipeline with one change: the ERP poster retries a timed-out post without looking it up first.</p>
 <div class="verdicts">{"".join(cards)}</div>
</section>

<section>
 <h2>Model trade-off</h2>
 <p>Business rules catch the extraction errors, so a weaker model does not post wrong money; it sends more clean invoices to people. Cost and review hours are projected to a 60,000-invoice month-end day at 90 seconds per review.</p>
 {table}
</section>

<section>
 <h2>Month-end day: people versus model spend</h2>
 <div class="grid2">
  <div class="bars"><h3>Review hours</h3>{chart_review}</div>
  <div class="bars"><h3>Model spend</h3>{chart_cost}</div>
 </div>
 <p class="note">Each chart shares one scale across models. A cheap model that doubles review hours costs more than it saves.</p>
</section>

<section>
 <h2>Detail by model</h2>
 <div class="tabs" role="tablist">{"".join(tabs)}</div>
 {"".join(panels)}
</section>

{scaling_html}
<section>
 <h2>Rough numbers</h2>
 <p>Production sizing with the lowest measured review rate ({pct(best["outcomes"]["review_rate"])}, {e(best["model"])}). The sample is defect-heavy by design, so production review rates should be lower.</p>
 <div class="scroll"><table><thead><tr><th>Quantity</th>{"".join(f'<th class="n">{e(k)}</th>' for k in est)}</tr></thead><tbody>{num_rows}</tbody></table></div>
</section>

<section>
 <h2>Run it yourself</h2>
<pre>pip install -r requirements.txt
python run.py gen --n 400
python run.py eval --chaos heavy
python run.py scale --workers 1,2,4,8,16,32
python run.py run --model claude-sonnet --fallback claude-haiku   # real models, needs ANTHROPIC_API_KEY</pre>
</section>
</main>
<script>
document.querySelectorAll('.tabs button').forEach(b => b.addEventListener('click', () => {{
  document.querySelectorAll('.tabs button').forEach(x => x.setAttribute('aria-selected', String(x === b)));
  document.querySelectorAll('[role=tabpanel]').forEach(p => p.hidden = p.id !== b.getAttribute('aria-controls'));
}}));
</script>"""
    out.write_text(head + body)
    return out
