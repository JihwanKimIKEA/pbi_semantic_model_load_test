"""Synthetic Performance Analyzer capture + the injected truth the harness checks against.

    python tests/synthetic_capture.py OUT.json     # writes a valid PowerBIPerformanceData.json

The capture has 4 clicks (user actions) with 8 visuals, 1-3 queries in parallel per click, linked like the real file:
User Action ... Visual Container Lifecycle -> Query -> Execute Semantic Query -> Execute DAX Query (parentId chain).
Every query starts with a "// vis:<id>" comment, so the fake engine knows which visual it serves (latency, CPU).

TRUTH is the single place for what the harness injects into model B (the candidate):
  - one visual clearly slower in B, one clearly faster in B (latency and server CPU),
  - one changed measure ([Margin]; [Margin %] depends on it),
  - a staler refresh and slightly fewer rows (B's data ends 9 days earlier),
  - late-arriving data: B's newest days are only partly loaded (the number-check window must end before them).
"""
import json
import sys
from datetime import datetime, timedelta, timezone

# ---- injected truth ----------------------------------------------------------------------------------------------
TRUTH = {
    "slow_visual": "v3",            # B ~2.5x slower and costlier
    "fast_visual": "v6",            # B ~2.5x faster and cheaper
    "changed_measure": "Margin",    # B: [Total Sales] - [Total Cost] * 1.02
    "dependent_measure": "Margin %",
    "a_data_lag_days": 1,           # A: last date with data = today - 1, refreshed ~6 h ago
    "b_data_lag_days": 10,          # B: last date with data = today - 10, refreshed 10 days ago (staler, fewer rows)
    "b_unsettled_days": 3,          # B: its 3 newest days miss one row per business unit and day (late-arriving data)
    "slow_click": 1,                # the click that contains the slow visual
    "fast_click": 3,                # the click that contains the fast visual
}

# ---- visuals: id, click, title, type, server latency ms (A, B), CPU per ms of server time (A, B), DAX ----------------
_SLICER = "VAR __DS0FilterTable = TREATAS({\"North\", \"South\", \"East\", \"West\", \"Metro\"}, 'Business Unit'[Business Unit])"


def _q(vis, core, filt=None, order=None):
    lines = [f"// vis:{vis}", "DEFINE"]
    if filt:
        lines.append("\t" + filt)
    lines.append(f"\tVAR __DS0Core = {core}")
    lines.append("EVALUATE")
    lines.append(f"\tTOPN(1001, __DS0Core, {order}, 1)" if order else "\t__DS0Core")
    if order:
        lines.append(f"ORDER BY {order}")
    return "\n".join(lines)


VISUALS = [
    {"id": "v1", "click": 1, "title": "Total Sales", "type": "card", "ms": (80, 80), "cpu": (1.5, 1.5),
     "dax": _q("v1", "SUMMARIZECOLUMNS(\"Total_Sales\", 'Sales'[Total Sales])")},
    {"id": "v2", "click": 1, "title": "Sales by Business Unit", "type": "clusteredBarChart", "ms": (150, 150), "cpu": (1.5, 1.5),
     "dax": _q("v2", "SUMMARIZECOLUMNS('Business Unit'[Business Unit], \"Total_Sales\", 'Sales'[Total Sales], "
                     "\"Margin\", 'Sales'[Margin])", order="'Business Unit'[Business Unit]")},
    {"id": "v3", "click": 1, "title": "Sales by Month", "type": "lineChart", "ms": (200, 500), "cpu": (1.5, 1.8),
     "dax": _q("v3", "SUMMARIZECOLUMNS('Date'[Month], \"Total_Sales\", 'Sales'[Total Sales], "
                     "\"Total_Quantity\", 'Sales'[Total Quantity])", order="'Date'[Month]")},
    {"id": "v4", "click": 2, "title": "Margin by Business Unit", "type": "tableEx", "ms": (120, 125), "cpu": (1.5, 1.5),
     "dax": _q("v4", "SUMMARIZECOLUMNS('Business Unit'[Business Unit], \"Margin\", 'Sales'[Margin], "
                     "\"Margin__\", 'Sales'[Margin %], \"Avg_Price\", 'Sales'[Avg Price])", order="'Business Unit'[Business Unit]")},
    {"id": "v5", "click": 2, "title": "Order Lines", "type": "card", "ms": (60, 60), "cpu": (1.5, 1.5),
     "dax": _q("v5", "SUMMARIZECOLUMNS(\"Order_Lines\", 'Sales'[Order Lines])")},
    {"id": "v6", "click": 3, "title": "Daily Sales", "type": "tableEx", "ms": (400, 160), "cpu": (1.5, 1.2),
     "dax": _q("v6", "SUMMARIZECOLUMNS('Date'[Date], __DS0FilterTable, \"Total_Sales\", 'Sales'[Total Sales], "
                     "\"Order_Lines\", 'Sales'[Order Lines])", filt=_SLICER, order="'Date'[Date]")},
    {"id": "v7", "click": 4, "title": "Units per Line by Business Unit", "type": "clusteredColumnChart", "ms": (180, 185),
     "cpu": (1.5, 1.5),
     "dax": _q("v7", "SUMMARIZECOLUMNS('Business Unit'[Business Unit], \"Units_per_Line\", 'Sales'[Units per Line], "
                     "\"Total_Cost\", 'Sales'[Total Cost])", order="'Business Unit'[Business Unit]")},
    {"id": "v8", "click": 4, "title": "Avg Price", "type": "card", "ms": (70, 70), "cpu": (1.5, 1.5),
     "dax": _q("v8", "SUMMARIZECOLUMNS(\"Avg_Price\", 'Sales'[Avg Price])")},
]
CLICKS = {1: "Refresh", 2: "ChangePage", 3: "ApplyFilter", 4: "Refresh"}
VISUAL_BY_ID = {v["id"]: v for v in VISUALS}


def build_capture(start=None):
    """The capture as a dict (Performance Analyzer export format)."""
    t = start or datetime(2026, 10, 1, 9, 0, 0, tzinfo=timezone.utc)
    iso = lambda d: d.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"  # noqa: E731
    events, n = [], [0]

    def ev(name, start_, end_, parent=None, metrics=None):
        n[0] += 1
        e = {"name": name, "component": "Report Canvas", "id": f"e{n[0]:04d}", "start": iso(start_), "end": iso(end_)}
        if parent:
            e["parentId"] = parent
        if metrics:
            e["metrics"] = metrics
        events.append(e)
        return e["id"]

    for click, label in CLICKS.items():
        ev("User Action", t, t + timedelta(milliseconds=5), metrics={"sourceLabel": f"UserAction_{label}"})
        t += timedelta(milliseconds=20)
        group_end = t
        for v in [v for v in VISUALS if v["click"] == click]:   # the queries of one click overlap (parallel)
            dur = timedelta(milliseconds=int(v["ms"][0] * 1.3) + 40)
            vcl = ev("Visual Container Lifecycle", t, t + dur + timedelta(milliseconds=60),
                     metrics={"visualTitle": v["title"], "visualType": v["type"], "visualId": f"{v['id']}-guid",
                              "initialLoad": True})
            qid = ev("Query", t, t + dur + timedelta(milliseconds=20), parent=vcl)
            esq = ev("Execute Semantic Query", t + timedelta(milliseconds=5), t + dur + timedelta(milliseconds=10), parent=qid)
            ev("Execute DAX Query", t + timedelta(milliseconds=8), t + timedelta(milliseconds=8) + dur, parent=esq,
               metrics={"QueryText": v["dax"], "RowCount": 0})
            ev("Render", t + dur + timedelta(milliseconds=20), t + dur + timedelta(milliseconds=55), parent=vcl)
            group_end = max(group_end, t + dur + timedelta(milliseconds=60))
            t += timedelta(milliseconds=3)
        t = group_end + timedelta(seconds=4)   # the person looks at the page before the next click
    return {"version": "1.1.0", "events": events}


def write_capture(path):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(build_capture(), fh, indent=1)
    return path


if __name__ == "__main__":
    print(write_capture(sys.argv[1] if len(sys.argv) > 1 else "PowerBIPerformanceData.json"))
