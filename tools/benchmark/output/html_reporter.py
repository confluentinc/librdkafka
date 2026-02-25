"""
Generate a self-contained HTML benchmark report.

Includes:
  - Benchmark metadata header
  - Per-run results table (one row per run, colour-coded by status)
  - Aggregated summary tables (Producer + Consumer)
  - Bar charts for throughput and latency across runs (Chart.js via CDN)

The HTML file is fully self-contained except for Chart.js loaded from CDN.
It renders correctly offline if the browser has the script cached.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from core.metrics import AggregatedField, BenchmarkSummary, RunResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt(v: Optional[float], dp: int = 1, fallback: str = "—") -> str:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return fallback
    if dp == 0:
        return f"{int(v):,}"
    return f"{v:,.{dp}f}"


def _agg_row(label: str, field: Optional[AggregatedField],
             dp: int = 1) -> str:
    if field is None:
        return f"<tr><td>{label}</td><td colspan='5' class='na'>—</td></tr>"
    return (
        f"<tr>"
        f"<td>{label}</td>"
        f"<td>{_fmt(field.mean, dp)}</td>"
        f"<td>{_fmt(field.stddev, dp)}</td>"
        f"<td>{_fmt(field.min, dp)}</td>"
        f"<td>{_fmt(field.max, dp)}</td>"
        f"<td>{_fmt(field.p50, dp)}</td>"
        f"</tr>"
    )


def _run_status_class(run: RunResult) -> str:
    return "run-ok" if run.status == "ok" else "run-fail"


def _safe(v: Optional[float], dp: int = 1) -> str:
    return _fmt(v, dp)


# ---------------------------------------------------------------------------
# Chart data helpers
# ---------------------------------------------------------------------------

def _chart_labels(runs: list) -> str:
    return json.dumps([f"Run {r.run_id}" for r in runs])


def _chart_vals(runs: list, getter, fallback: float = 0.0) -> str:
    vals = []
    for r in runs:
        try:
            v = getter(r)
            vals.append(round(v, 2) if v is not None else fallback)
        except Exception:
            vals.append(fallback)
    return json.dumps(vals)


def _chart_colors(runs: list) -> str:
    colors = [
        "rgba(54,162,235,0.8)" if r.status == "ok" else "rgba(255,99,132,0.8)"
        for r in runs
    ]
    return json.dumps(colors)


# ---------------------------------------------------------------------------
# HTML sections
# ---------------------------------------------------------------------------

def _css() -> str:
    return """
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background: #f4f6f9; color: #2d3748; padding: 24px; }
    h1 { font-size: 1.6rem; font-weight: 700; margin-bottom: 4px; }
    h2 { font-size: 1.1rem; font-weight: 600; margin: 24px 0 10px;
         color: #4a5568; border-bottom: 2px solid #e2e8f0; padding-bottom: 4px; }
    .meta { font-size: 0.85rem; color: #718096; margin-bottom: 20px; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 12px;
             font-size: 0.78rem; font-weight: 600; }
    .badge-ok   { background: #c6f6d5; color: #276749; }
    .badge-fail { background: #fed7d7; color: #9b2c2c; }
    .cards { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }
    .card { background: white; border-radius: 10px; padding: 16px 20px;
            box-shadow: 0 1px 4px rgba(0,0,0,.08); min-width: 150px; flex: 1; }
    .card-label { font-size: 0.75rem; color: #718096; text-transform: uppercase;
                  letter-spacing: .05em; margin-bottom: 4px; }
    .card-value { font-size: 1.4rem; font-weight: 700; }
    table { width: 100%; border-collapse: collapse; background: white;
            border-radius: 10px; overflow: hidden;
            box-shadow: 0 1px 4px rgba(0,0,0,.08); margin-bottom: 24px; }
    th { background: #2d3748; color: white; font-size: 0.78rem;
         text-align: left; padding: 10px 12px; font-weight: 600; }
    td { padding: 9px 12px; font-size: 0.84rem; border-bottom: 1px solid #edf2f7; }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #f7fafc; }
    .run-ok  td:first-child { border-left: 3px solid #48bb78; }
    .run-fail td:first-child { border-left: 3px solid #fc8181; }
    .na { color: #a0aec0; }
    .charts { display: grid; grid-template-columns: 1fr 1fr; gap: 20px;
              margin-bottom: 24px; }
    .chart-box { background: white; border-radius: 10px; padding: 16px;
                 box-shadow: 0 1px 4px rgba(0,0,0,.08); }
    .chart-title { font-size: 0.85rem; font-weight: 600; color: #4a5568;
                   margin-bottom: 10px; }
    .section { margin-bottom: 8px; }
    .config-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px,1fr));
                   gap: 10px; background: white; border-radius: 10px; padding: 16px;
                   box-shadow: 0 1px 4px rgba(0,0,0,.08); margin-bottom: 24px; }
    .config-item { font-size: 0.82rem; }
    .config-key   { color: #718096; font-weight: 500; }
    .config-val   { color: #2d3748; font-weight: 600; }
    footer { font-size: 0.75rem; color: #a0aec0; margin-top: 24px; text-align: center; }
    @media (max-width: 700px) { .charts { grid-template-columns: 1fr; } }
    """


def _header(summary: BenchmarkSummary) -> str:
    status_badge = (
        f'<span class="badge badge-ok">{summary.runs_completed} ok</span>'
        if summary.runs_failed == 0
        else f'<span class="badge badge-ok">{summary.runs_completed} ok</span> '
             f'<span class="badge badge-fail">{summary.runs_failed} failed</span>'
    )
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    mode = (
        f"time-driven ({summary.runs[0].producer.elapsed_sec:.0f}s)"
        if summary.runs and summary.runs[0].producer and summary.runs[0].producer.elapsed_sec
        else "count-driven"
    )

    return f"""
    <h1>Kafka Benchmark Report</h1>
    <p class="meta">Generated {ts}</p>

    <div class="cards">
      <div class="card">
        <div class="card-label">Preset</div>
        <div class="card-value">{summary.preset}</div>
      </div>
      <div class="card">
        <div class="card-label">Runs</div>
        <div class="card-value">{status_badge}</div>
      </div>
      <div class="card">
        <div class="card-label">Broker</div>
        <div class="card-value" style="font-size:1rem">{summary.broker}</div>
      </div>
      <div class="card">
        <div class="card-label">Client</div>
        <div class="card-value" style="font-size:0.9rem">{summary.client_impl}</div>
      </div>
      <div class="card">
        <div class="card-label">Msg size</div>
        <div class="card-value">{summary.msg_size:,} B</div>
      </div>
    </div>
    """


def _config_section(config: dict) -> str:
    w = config.get("workload", {})
    p = config.get("producer", {})
    c = config.get("consumer", {})

    items = [
        ("compression", w.get("compression", "—")),
        ("acks", p.get("acks", "—")),
        ("linger_ms", p.get("linger_ms", "—")),
        ("batch_size", f"{p.get('batch_size', 0):,} B"),
        ("idempotent", "yes" if p.get("enable_idempotence") else "no"),
        ("EOS (txn)", "yes" if p.get("transactional_id") else "no"),
        ("fetch_min_bytes", c.get("fetch_min_bytes", "—")),
        ("queued_min_msgs", f"{c.get('queued_min_messages', 0):,}"),
        ("num_partitions", w.get("num_partitions", "—")),
        ("replication_factor", w.get("replication_factor", "—")),
    ]

    rows = "".join(
        f'<div class="config-item">'
        f'<span class="config-key">{k}</span>: '
        f'<span class="config-val">{v}</span>'
        f'</div>'
        for k, v in items
    )
    return f"""
    <h2>Configuration</h2>
    <div class="config-grid">{rows}</div>
    """


def _per_run_table(runs: list) -> str:
    rows = []
    for r in runs:
        p = r.producer
        c = r.consumer
        status_badge = (
            '<span class="badge badge-ok">OK</span>'
            if r.status == "ok"
            else f'<span class="badge badge-fail">FAIL</span>'
        )
        err = f'<br><small style="color:#a0aec0">{r.error[:80]}...</small>' if r.error else ""
        rows.append(
            f'<tr class="{_run_status_class(r)}">'
            f'<td>{r.run_id}</td>'
            f'<td>{status_badge}{err}</td>'
            f'<td>{_safe(p.msgs_per_sec, 0) if p else "—"}</td>'
            f'<td>{_safe(p.mb_per_sec, 2) if p else "—"}</td>'
            f'<td>{_safe(p.dr_latency_avg_us) if p else "—"}</td>'
            f'<td>{_safe(p.dr_latency_p99_us) if p else "—"}</td>'
            f'<td>{_safe(p.cpu_avg_pct) if p else "—"}</td>'
            f'<td>{_safe(p.mem_peak_mb) if p else "—"}</td>'
            f'<td>{_safe(c.msgs_per_sec, 0) if c else "—"}</td>'
            f'<td>{_safe(c.mb_per_sec, 2) if c else "—"}</td>'
            f'<td>{_safe(c.e2e_latency_avg_ms) if c else "—"}</td>'
            f'<td>{_safe(c.e2e_latency_p99_ms) if c else "—"}</td>'
            f'<td>{_safe(c.cpu_avg_pct) if c else "—"}</td>'
            f'<td>{_safe(c.mem_peak_mb) if c else "—"}</td>'
            f'<td>{_safe(c.consumer_lag_at_end, 0) if c else "—"}</td>'
            f'</tr>'
        )

    return f"""
    <h2>Per-Run Results</h2>
    <table>
      <thead>
        <tr>
          <th>Run</th><th>Status</th>
          <th>P msgs/s</th><th>P MB/s</th>
          <th>DR avg (µs)</th><th>DR p99 (µs)</th>
          <th>P CPU avg%†</th><th>P mem peak MB†</th>
          <th>C msgs/s</th><th>C MB/s</th>
          <th>E2E avg (ms)</th><th>E2E p99 (ms)</th>
          <th>C CPU avg%†</th><th>C mem peak MB†</th>
          <th>C-lag</th>
        </tr>
      </thead>
      <tbody>{"".join(rows)}</tbody>
    </table>
    """


def _resource_callout(client_impl: str) -> str:
    # Local builds (C and C++) accurately reflect librdkafka's own usage.
    # System/bundled builds (confluent Python, confluent_cpp) include runtime overhead.
    is_local_build = "local build" in client_impl
    if is_local_build:
        note = (
            "<strong>Local build client (rdkafka_perf / rdkafka_perf_cpp):</strong> "
            "CPU and memory are sampled on the benchmark binary process tree. "
            "This closely reflects librdkafka's own resource usage."
        )
        color = "#c6f6d5"
        border = "#276749"
    else:
        note = (
            "<strong>Bundled/system librdkafka client:</strong> "
            "For the Python <em>confluent</em> client, CPU and memory include Python "
            "interpreter overhead (~30&ndash;50 MB RSS baseline, ~5&ndash;15% CPU for the GIL/runtime). "
            "For the C++ <em>confluent_cpp</em> client, figures reflect the system-installed "
            "librdkafka, not your local source changes. "
            "Use <code>--client rdkafka_perf</code> or <code>--client rdkafka_perf_cpp</code> "
            "for accurate local-build measurements."
        )
        color = "#fefcbf"
        border = "#d69e2e"
    return (
        f'<div style="background:{color};border-left:4px solid {border};'
        f'padding:10px 14px;border-radius:6px;font-size:0.83rem;'
        f'margin-bottom:20px">'
        f'† CPU/memory accuracy note: {note}'
        f"</div>"
    )


def _aggregated_tables(summary: BenchmarkSummary) -> str:
    agg_header = """
    <thead>
      <tr>
        <th>Metric</th><th>mean</th><th>stddev</th>
        <th>min</th><th>max</th><th>p50</th>
      </tr>
    </thead>"""

    has_resource = (
        summary.producer and summary.producer.cpu_avg_pct is not None
        or summary.consumer and summary.consumer.cpu_avg_pct is not None
    )
    callout = _resource_callout(summary.client_impl) if has_resource else ""

    prod_html = ""
    if summary.producer:
        p = summary.producer
        cpu_rows = ""
        if p.cpu_avg_pct is not None:
            cpu_rows = f"""
        {_agg_row("CPU avg (%)†",   p.cpu_avg_pct, dp=1)}
        {_agg_row("CPU max (%)†",   p.cpu_max_pct, dp=1)}
        {_agg_row("mem peak (MB)†", p.mem_peak_mb, dp=1)}"""
        prod_html = f"""
    <h2>Producer — Aggregated</h2>
    <table>
      {agg_header}
      <tbody>
        {_agg_row("msgs/s",          p.msgs_per_sec,       dp=0)}
        {_agg_row("MB/s",            p.mb_per_sec,         dp=2)}
        {_agg_row("DR lat avg (µs)", p.dr_latency_avg_us,  dp=1)}
        {_agg_row("DR lat p99 (µs)", p.dr_latency_p99_us,  dp=1)}
        {_agg_row("msgs failed",     p.msgs_failed,        dp=0)}
        {cpu_rows}
      </tbody>
    </table>"""

    cons_html = ""
    if summary.consumer:
        c = summary.consumer
        cpu_rows = ""
        if c.cpu_avg_pct is not None:
            cpu_rows = f"""
        {_agg_row("CPU avg (%)†",   c.cpu_avg_pct, dp=1)}
        {_agg_row("CPU max (%)†",   c.cpu_max_pct, dp=1)}
        {_agg_row("mem peak (MB)†", c.mem_peak_mb, dp=1)}"""
        cons_html = f"""
    <h2>Consumer — Aggregated</h2>
    <table>
      {agg_header}
      <tbody>
        {_agg_row("msgs/s",           c.msgs_per_sec,        dp=0)}
        {_agg_row("MB/s",             c.mb_per_sec,          dp=2)}
        {_agg_row("E2E lat avg (ms)", c.e2e_latency_avg_ms,  dp=2)}
        {_agg_row("E2E lat p99 (ms)", c.e2e_latency_p99_ms,  dp=2)}
        {_agg_row("lag at end",       c.consumer_lag_at_end, dp=0)}
        {cpu_rows}
      </tbody>
    </table>"""

    return callout + prod_html + cons_html


def _charts(runs: list) -> str:
    ok_runs = [r for r in runs if r.status == "ok"]
    if not ok_runs:
        return ""

    labels = _chart_labels(ok_runs)
    colors = _chart_colors(ok_runs)

    p_msgs  = _chart_vals(ok_runs, lambda r: r.producer.msgs_per_sec)
    p_mb    = _chart_vals(ok_runs, lambda r: r.producer.mb_per_sec)
    c_msgs  = _chart_vals(ok_runs, lambda r: r.consumer.msgs_per_sec)
    c_mb    = _chart_vals(ok_runs, lambda r: r.consumer.mb_per_sec)
    dr_avg  = _chart_vals(ok_runs, lambda r: r.producer.dr_latency_avg_us)
    dr_p99  = _chart_vals(ok_runs, lambda r: r.producer.dr_latency_p99_us)
    e2e_avg = _chart_vals(ok_runs, lambda r: r.consumer.e2e_latency_avg_ms)
    e2e_p99 = _chart_vals(ok_runs, lambda r: r.consumer.e2e_latency_p99_ms)

    def bar_chart(canvas_id: str, title: str, datasets: list) -> str:
        ds_json = json.dumps(datasets)
        return f"""
        <div class="chart-box">
          <div class="chart-title">{title}</div>
          <canvas id="{canvas_id}"></canvas>
          <script>
            new Chart(document.getElementById('{canvas_id}'), {{
              type: 'bar',
              data: {{
                labels: {labels},
                datasets: {ds_json}
              }},
              options: {{
                responsive: true,
                plugins: {{ legend: {{ position: 'bottom' }} }},
                scales: {{ y: {{ beginAtZero: true }} }}
              }}
            }});
          </script>
        </div>"""

    chart_throughput = bar_chart("chart_throughput", "Throughput — msgs/s", [
        {"label": "Producer msgs/s", "data": json.loads(p_msgs),
         "backgroundColor": "rgba(54,162,235,0.75)"},
        {"label": "Consumer msgs/s", "data": json.loads(c_msgs),
         "backgroundColor": "rgba(255,159,64,0.75)"},
    ])

    chart_mbps = bar_chart("chart_mbps", "Throughput — MB/s", [
        {"label": "Producer MB/s", "data": json.loads(p_mb),
         "backgroundColor": "rgba(54,162,235,0.75)"},
        {"label": "Consumer MB/s", "data": json.loads(c_mb),
         "backgroundColor": "rgba(255,159,64,0.75)"},
    ])

    chart_dr = bar_chart("chart_dr", "Producer DR Latency (µs)", [
        {"label": "avg (µs)", "data": json.loads(dr_avg),
         "backgroundColor": "rgba(75,192,192,0.75)"},
        {"label": "p99 (µs)", "data": json.loads(dr_p99),
         "backgroundColor": "rgba(153,102,255,0.75)"},
    ])

    chart_e2e = bar_chart("chart_e2e", "E2E Latency (ms)", [
        {"label": "avg (ms)", "data": json.loads(e2e_avg),
         "backgroundColor": "rgba(75,192,192,0.75)"},
        {"label": "p99 (ms)", "data": json.loads(e2e_p99),
         "backgroundColor": "rgba(153,102,255,0.75)"},
    ])

    return f"""
    <h2>Charts</h2>
    <div class="charts">
      {chart_throughput}
      {chart_mbps}
      {chart_dr}
      {chart_e2e}
    </div>
    """


def _build_html(summary: BenchmarkSummary, config: dict) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Benchmark: {summary.preset}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>{_css()}</style>
</head>
<body>
  {_header(summary)}
  {_config_section(config)}
  <h2>Charts</h2>
  {_charts(summary.runs)}
  {_per_run_table(summary.runs)}
  {_aggregated_tables(summary)}
  <footer>
    Generated by librdkafka benchmark tool &mdash;
    <a href="https://github.com/confluentinc/librdkafka">github.com/confluentinc/librdkafka</a>
  </footer>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def write(summary: BenchmarkSummary, config: dict, output_dir: Path) -> Path:
    """Generate an HTML report. Returns the path to the written file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"benchmark_{summary.preset}_{ts}.html"
    path = output_dir / filename

    with open(path, "w", encoding="utf-8") as f:
        f.write(_build_html(summary, config))

    return path
