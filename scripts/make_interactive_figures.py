#!/usr/bin/env python3
"""Build small self-contained HTML companions from public CSV tables."""

from __future__ import annotations

import csv
import html
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TABLES = ROOT / "artifacts" / "tables"
OUT = ROOT / "artifacts" / "figures_html"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def page(title: str, body: str, payload: object | None = None) -> str:
    data = "" if payload is None else f"<script>const DATA = {json.dumps(payload)};</script>"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, sans-serif; margin: 32px; color: #1f2933; }}
    h1 {{ font-size: 28px; margin-bottom: 8px; }}
    .note {{ color: #52606d; max-width: 900px; }}
    svg {{ width: min(100%, 1100px); height: 520px; border: 1px solid #d9e2ec; background: #fff; }}
    button, select, label {{ font: inherit; margin: 6px 8px 12px 0; }}
    table {{ border-collapse: collapse; margin-top: 16px; font-size: 14px; }}
    th, td {{ border: 1px solid #d9e2ec; padding: 6px 9px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
  </style>
</head>
<body>
{body}
{data}
</body>
</html>
"""


def svg_line_chart(series: dict[str, list[float]], x_labels: list[str], title: str) -> str:
    width, height = 1040, 460
    left, right, top, bottom = 70, 30, 40, 55
    vals = [v for ys in series.values() for v in ys]
    lo, hi = min(vals), max(vals)
    pad = (hi - lo) * 0.08 or 1.0
    lo -= pad
    hi += pad
    colors = ["#1f77b4", "#2ca02c", "#9467bd", "#d62728", "#ff7f0e"]

    def xy(i: int, value: float) -> tuple[float, float]:
        x = left + i * (width - left - right) / max(1, len(x_labels) - 1)
        y = top + (hi - value) * (height - top - bottom) / (hi - lo)
        return x, y

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        f'<text x="{width / 2}" y="24" text-anchor="middle" font-size="20">{html.escape(title)}</text>',
        f'<line x1="{left}" y1="{height-bottom}" x2="{width-right}" y2="{height-bottom}" stroke="#52606d"/>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height-bottom}" stroke="#52606d"/>',
    ]
    for tick in range(5):
        val = lo + tick * (hi - lo) / 4
        y = top + (hi - val) * (height - top - bottom) / (hi - lo)
        parts.append(f'<line x1="{left-4}" y1="{y:.1f}" x2="{left}" y2="{y:.1f}" stroke="#52606d"/>')
        parts.append(f'<text x="{left-8}" y="{y+4:.1f}" text-anchor="end" font-size="12">{val:.3f}</text>')
    for idx in [0, len(x_labels) // 2, len(x_labels) - 1]:
        x, _ = xy(idx, lo)
        parts.append(f'<text x="{x:.1f}" y="{height-18}" text-anchor="middle" font-size="12">{html.escape(x_labels[idx])}</text>')
    for j, (name, ys) in enumerate(series.items()):
        points = " ".join(f"{x:.1f},{y:.1f}" for x, y in (xy(i, v) for i, v in enumerate(ys)))
        color = colors[j % len(colors)]
        parts.append(f'<polyline fill="none" stroke="{color}" stroke-width="3" points="{points}"/>')
        parts.append(f'<text x="{left + 10}" y="{top + 22 + 20*j}" fill="{color}" font-size="14">{html.escape(name)}</text>')
    parts.append("</svg>")
    return "\n".join(parts)


def write_cumulative() -> None:
    rows = read_csv(TABLES / "decile_returns.csv")
    labels = [r["ym"] for r in rows]
    series = {
        "Decile 1": [float(r["1.0"]) for r in rows],
        "Decile 10": [float(r["10.0"]) for r in rows],
        "Long-short cumulative": [float(r["cum_long_short_10_1"]) for r in rows],
    }
    body = "<h1>Cumulative LIIS Return Companion</h1><p class='note'>Public CSV companion for the decile and long-short return tables.</p>"
    body += svg_line_chart(series, labels, "LIIS Decile and Long-Short Series")
    OUT.joinpath("cumulative_liis_return.html").write_text(page("Cumulative LIIS Return", body), encoding="utf-8")


def write_event_car() -> None:
    rows = read_csv(TABLES / "event_car_by_decile.csv")
    days = [k for k in rows[0].keys() if k != "event_decile"]
    selected = {f"Decile {int(float(r['event_decile']))}": [float(r[d]) for d in days] for r in rows if r["event_decile"] in {"1.0", "5.0", "10.0"}}
    body = "<h1>Event-Time CAR Companion</h1><p class='note'>Cumulative abnormal returns around filing availability for selected LIIS deciles.</p>"
    body += svg_line_chart(selected, days, "Event-Time CAR by LIIS Decile")
    OUT.joinpath("event_car_by_decile.html").write_text(page("Event-Time CAR", body), encoding="utf-8")


def write_robustness() -> None:
    rows = read_csv(TABLES / "robustness_summary.csv")
    headers = ["test", "status", "months", "mean_long_short", "t_stat"]
    table = ["<table><thead><tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr></thead><tbody>"]
    for row in rows:
        table.append("<tr>" + "".join(f"<td>{html.escape(row[h])}</td>" for h in headers) + "</tr>")
    table.append("</tbody></table>")
    body = "<h1>Robustness Dashboard</h1><p class='note'>Public robustness table generated from cached proposal-grade outputs.</p>"
    body += "\n".join(table)
    OUT.joinpath("robustness_dashboard.html").write_text(page("Robustness Dashboard", body), encoding="utf-8")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    write_cumulative()
    write_event_car()
    write_robustness()
    print(f"wrote_html_companions={OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
