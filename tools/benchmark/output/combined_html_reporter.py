"""
Generate a single combined HTML report from multiple per-preset JSON result files.

Reads all benchmark_*.json files in a directory and produces one HTML file with:
  - Side-by-side comparison charts (throughput, latency) for all presets
  - Overview comparison table — one row per preset
  - Per-preset detail sections — run table + aggregated stats
"""

from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# JSON loading
# ---------------------------------------------------------------------------

def load_results(output_dir: Path) -> List[Dict[str, Any]]:
    """Load and return all benchmark JSON files, sorted by preset name."""
    files = sorted(output_dir.glob("benchmark_*.json"))
    results = []
    for f in files:
        try:
            with open(f) as fp:
                data = json.load(fp)
            results.append(data)
        except Exception as e:
            print(f"  [warn] Could not load {f.name}: {e}")
    return results


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt(v: Optional[float], dp: int = 1) -> str:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return "—"
    if dp == 0:
        return f"{int(v):,}"
    return f"{v:,.{dp}f}"


def _agg_val(result: dict, side: str, field: str) -> Optional[float]:
    """Extract aggregated mean for a given side (producer/consumer) and field."""
    try:
        return result["aggregated"][side][field]["mean"]
    except (KeyError, TypeError):
        return None


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

def _css() -> str:
    return """
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background: #f4f6f9; color: #2d3748; padding: 24px; }
    h1  { font-size: 1.6rem; font-weight: 700; margin-bottom: 4px; }
    h2  { font-size: 1.1rem; font-weight: 600; margin: 28px 0 10px;
           color: #4a5568; border-bottom: 2px solid #e2e8f0; padding-bottom: 4px; }
    h3  { font-size: 0.95rem; font-weight: 600; margin: 20px 0 8px; color: #2d3748; }
    .meta { font-size: 0.85rem; color: #718096; margin-bottom: 20px; }
    .cards { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }
    .card { background: white; border-radius: 10px; padding: 16px 20px;
            box-shadow: 0 1px 4px rgba(0,0,0,.08); min-width: 140px; flex: 1; }
    .card-label { font-size: 0.75rem; color: #718096; text-transform: uppercase;
                  letter-spacing:.05em; margin-bottom: 4px; }
    .card-value { font-size: 1.3rem; font-weight: 700; }
    .badge { display:inline-block; padding:2px 8px; border-radius:12px;
             font-size:.78rem; font-weight:600; }
    .badge-ok   { background:#c6f6d5; color:#276749; }
    .badge-fail { background:#fed7d7; color:#9b2c2c; }
    table { width:100%; border-collapse:collapse; background:white;
            border-radius:10px; overflow:hidden;
            box-shadow:0 1px 4px rgba(0,0,0,.08); margin-bottom:24px; }
    th { background:#2d3748; color:white; font-size:.78rem;
         text-align:left; padding:10px 12px; font-weight:600; }
    td { padding:8px 12px; font-size:.83rem; border-bottom:1px solid #edf2f7; }
    tr:last-child td { border-bottom:none; }
    tr:hover td { background:#f7fafc; }
    .na { color:#a0aec0; }
    .charts-grid { display:grid; grid-template-columns:1fr 1fr; gap:20px;
                   margin-bottom:28px; }
    .chart-box { background:white; border-radius:10px; padding:16px;
                 box-shadow:0 1px 4px rgba(0,0,0,.08); }
    .chart-title { font-size:.85rem; font-weight:600; color:#4a5568; margin-bottom:10px; }
    .preset-section { background:white; border-radius:10px; padding:20px;
                      box-shadow:0 1px 4px rgba(0,0,0,.08); margin-bottom:20px; }
    .preset-section h3 { margin-top:0; font-size:1rem; border-bottom:1px solid #e2e8f0;
                          padding-bottom:8px; margin-bottom:12px; }
    .run-ok  td:first-child { border-left:3px solid #48bb78; }
    .run-fail td:first-child { border-left:3px solid #fc8181; }
    footer { font-size:.75rem; color:#a0aec0; margin-top:28px; text-align:center; }
    @media (max-width:700px) { .charts-grid { grid-template-columns:1fr; } }
    """


# ---------------------------------------------------------------------------
# Comparison charts (all presets side by side)
# ---------------------------------------------------------------------------

_CHART_COLORS = [
    "rgba(54,162,235,0.8)",
    "rgba(255,159,64,0.8)",
    "rgba(75,192,192,0.8)",
    "rgba(153,102,255,0.8)",
    "rgba(255,99,132,0.8)",
    "rgba(255,205,86,0.8)",
]


def _comparison_charts(results: list) -> str:
    labels = json.dumps([r["preset"] for r in results])

    def _vals(side: str, field: str) -> str:
        return json.dumps([
            round(_agg_val(r, side, field) or 0, 2) for r in results
        ])

    def bar_chart(cid: str, title: str, label: str, vals_json: str,
                  color: str = "rgba(54,162,235,0.8)") -> str:
        return f"""
        <div class="chart-box">
          <div class="chart-title">{title}</div>
          <canvas id="{cid}"></canvas>
          <script>
            new Chart(document.getElementById('{cid}'), {{
              type: 'bar',
              data: {{
                labels: {labels},
                datasets: [{{
                  label: '{label}',
                  data: {vals_json},
                  backgroundColor: {json.dumps([_CHART_COLORS[i % len(_CHART_COLORS)]
                                                for i in range(len(results))])}
                }}]
              }},
              options: {{
                responsive: true,
                plugins: {{ legend: {{ display: false }} }},
                scales: {{ y: {{ beginAtZero: true }} }}
              }}
            }});
          </script>
        </div>"""

    p_msgs  = _vals("producer", "msgs_per_sec")
    c_msgs  = _vals("consumer", "msgs_per_sec")
    dr_p99  = _vals("producer", "dr_latency_p99_us")
    e2e_p99 = _vals("consumer", "e2e_latency_p99_ms")

    return f"""
    <h2>Comparison Charts</h2>
    <div class="charts-grid">
      {bar_chart("cmp_p_msgs",  "Producer msgs/s (mean)",      "msgs/s",  p_msgs)}
      {bar_chart("cmp_c_msgs",  "Consumer msgs/s (mean)",      "msgs/s",  c_msgs,  "rgba(255,159,64,0.8)")}
      {bar_chart("cmp_dr_p99",  "Producer DR Latency p99 (µs)","µs",      dr_p99,  "rgba(153,102,255,0.8)")}
      {bar_chart("cmp_e2e_p99", "E2E Latency p99 (ms)",        "ms",      e2e_p99, "rgba(255,99,132,0.8)")}
    </div>"""


# ---------------------------------------------------------------------------
# Overview comparison table
# ---------------------------------------------------------------------------

def _overview_table(results: list) -> str:
    rows = []
    for r in results:
        ok    = r.get("runs_completed", 0)
        total = r.get("runs_requested", 0)
        fail  = r.get("runs_failed", 0)
        status = (
            f'<span class="badge badge-ok">{ok}/{total} ok</span>'
            if fail == 0
            else f'<span class="badge badge-ok">{ok}</span> '
                 f'<span class="badge badge-fail">{fail} failed</span>'
        )
        rows.append(
            f"<tr>"
            f"<td><strong>{r['preset']}</strong></td>"
            f"<td>{status}</td>"
            f"<td>{_fmt(_agg_val(r, 'producer', 'msgs_per_sec'), 0)}</td>"
            f"<td>{_fmt(_agg_val(r, 'producer', 'mb_per_sec'), 2)}</td>"
            f"<td>{_fmt(_agg_val(r, 'producer', 'dr_latency_avg_us'), 1)}</td>"
            f"<td>{_fmt(_agg_val(r, 'producer', 'dr_latency_p99_us'), 1)}</td>"
            f"<td>{_fmt(_agg_val(r, 'consumer', 'msgs_per_sec'), 0)}</td>"
            f"<td>{_fmt(_agg_val(r, 'consumer', 'mb_per_sec'), 2)}</td>"
            f"<td>{_fmt(_agg_val(r, 'consumer', 'e2e_latency_avg_ms'), 2)}</td>"
            f"<td>{_fmt(_agg_val(r, 'consumer', 'e2e_latency_p99_ms'), 2)}</td>"
            f"</tr>"
        )

    return f"""
    <h2>Overview — All Presets</h2>
    <table>
      <thead>
        <tr>
          <th>Preset</th><th>Status</th>
          <th>P msgs/s</th><th>P MB/s</th>
          <th>DR avg (µs)</th><th>DR p99 (µs)</th>
          <th>C msgs/s</th><th>C MB/s</th>
          <th>E2E avg (ms)</th><th>E2E p99 (ms)</th>
        </tr>
      </thead>
      <tbody>{"".join(rows)}</tbody>
    </table>"""


# ---------------------------------------------------------------------------
# Per-preset detail section
# ---------------------------------------------------------------------------

def _preset_section(r: dict) -> str:
    preset  = r["preset"]
    broker  = r.get("broker", "—")
    client  = r.get("client_impl", "—")
    m_count = r.get("msg_count", 0)
    m_size  = r.get("msg_size", 0)

    # Per-run table
    run_rows = []
    for run in r.get("runs", []):
        p = run.get("producer") or {}
        c = run.get("consumer") or {}
        status  = run.get("status", "?")
        css_cls = "run-ok" if status == "ok" else "run-fail"
        badge   = (
            '<span class="badge badge-ok">OK</span>'
            if status == "ok"
            else '<span class="badge badge-fail">FAIL</span>'
        )
        err = run.get("error") or ""
        err_html = f'<br><small style="color:#a0aec0">{err[:80]}</small>' if err else ""
        run_rows.append(
            f'<tr class="{css_cls}">'
            f'<td>{run.get("run_id","?")}</td>'
            f'<td>{badge}{err_html}</td>'
            f'<td>{_fmt(p.get("msgs_per_sec"), 0)}</td>'
            f'<td>{_fmt(p.get("mb_per_sec"), 2)}</td>'
            f'<td>{_fmt(p.get("dr_latency_avg_us"))}</td>'
            f'<td>{_fmt(p.get("dr_latency_p99_us"))}</td>'
            f'<td>{_fmt(c.get("msgs_per_sec"), 0)}</td>'
            f'<td>{_fmt(c.get("mb_per_sec"), 2)}</td>'
            f'<td>{_fmt(c.get("e2e_latency_avg_ms"))}</td>'
            f'<td>{_fmt(c.get("e2e_latency_p99_ms"))}</td>'
            f'</tr>'
        )

    return f"""
    <div class="preset-section">
      <h3>{preset}
        <span style="font-weight:400;color:#718096;font-size:.85rem;margin-left:8px">
          broker: {broker} &nbsp;|&nbsp; client: {client}
          &nbsp;|&nbsp; msg_size: {m_size:,} B
          &nbsp;|&nbsp; msg_count: {m_count:,}
        </span>
      </h3>
      <table>
        <thead>
          <tr>
            <th>Run</th><th>Status</th>
            <th>P msgs/s</th><th>P MB/s</th>
            <th>DR avg (µs)</th><th>DR p99 (µs)</th>
            <th>C msgs/s</th><th>C MB/s</th>
            <th>E2E avg (ms)</th><th>E2E p99 (ms)</th>
          </tr>
        </thead>
        <tbody>{"".join(run_rows)}</tbody>
      </table>
    </div>"""


# ---------------------------------------------------------------------------
# Full HTML assembly
# ---------------------------------------------------------------------------

def _build_html(results: list, output_dir: Path) -> str:
    ts      = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    presets = [r["preset"] for r in results]
    clients = list({r.get("client_impl", "—") for r in results})

    total_ok   = sum(r.get("runs_completed", 0) for r in results)
    total_fail = sum(r.get("runs_failed",   0) for r in results)

    header = f"""
    <h1>Kafka Benchmark — Combined Report</h1>
    <p class="meta">Generated {ts} &nbsp;|&nbsp; {output_dir.name}</p>
    <div class="cards">
      <div class="card">
        <div class="card-label">Presets</div>
        <div class="card-value">{len(presets)}</div>
      </div>
      <div class="card">
        <div class="card-label">Total runs</div>
        <div class="card-value">
          <span class="badge badge-ok">{total_ok} ok</span>
          {"<span class='badge badge-fail'>" + str(total_fail) + " failed</span>" if total_fail else ""}
        </div>
      </div>
      <div class="card">
        <div class="card-label">Client</div>
        <div class="card-value" style="font-size:.95rem">{", ".join(clients)}</div>
      </div>
    </div>"""

    charts   = _comparison_charts(results) if results else ""
    overview = _overview_table(results) if results else ""
    details  = "\n".join(_preset_section(r) for r in results)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Benchmark: Combined Report</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>{_css()}</style>
</head>
<body>
  {header}
  {charts}
  {overview}
  <h2>Per-Preset Details</h2>
  {details}
  <footer>
    Generated by librdkafka benchmark tool &mdash;
    <a href="https://github.com/confluentinc/librdkafka">github.com/confluentinc/librdkafka</a>
  </footer>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Public entry point (called from combine_results.py and run_all_presets.sh)
# ---------------------------------------------------------------------------

def write(output_dir: Path) -> Optional[Path]:
    """
    Read all benchmark_*.json files in output_dir and write combined_report.html.
    Returns the path, or None if no JSON files were found.
    """
    results = load_results(output_dir)
    if not results:
        print(f"  [warn] No benchmark JSON files found in {output_dir}")
        return None

    html  = _build_html(results, output_dir)
    path  = output_dir / "combined_report.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path
