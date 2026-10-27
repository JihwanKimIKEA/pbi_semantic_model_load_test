"""Offline stand-ins for what exists only in Fabric: sempy.fabric, notebookutils, the .NET modules (System,
Microsoft.AnalysisServices.AdomdClient) and the semantic models behind them.

install(engine, lakehouse_dir) puts the stubs into sys.modules before the notebook cells run, and returns the
notebookutils object the cells use as a global (Fabric injects it the same way).

The fake engine (FakeEngine) holds one small synthetic model per side (A, B): a Sales fact (Date, BU Code, Amount,
Quantity, Cost), a Date table (marked as date table) and a Business Unit table, 8 measures, 2 relationships, and the
INFO.* catalog. It answers, by the exact DAX templates the notebook sends:
  - EVALUATE {1}; ROW(...) with COUNTROWS(INFO.x()), MAXX(INFO.PARTITIONS(), [RefreshedTime]), DISTINCTCOUNT(col),
    COUNTROWS('Table'), MAXX(FILTER(VALUES(date), [date < DATE() &&] NOT ISEMPTY(CALCULATETABLE('T'))), date)
  - SELECTCOLUMNS(INFO.x() | FILTER(INFO.x(), [Col] =/<> v), "alias", expr, ...) and FILTER(INFO.x(), ...)
  - SUMMARIZECOLUMNS(group cols, ROLLUPADDISSUBTOTAL(col, "flag"), filter VARs (TREATAS / date window),
    "alias", measure | COUNTROWS('T'), ...): the number check and every replayed report query
Anything else raises "unsupported query (fake engine)" and is listed in engine.unsupported, so a new DAX template in
the notebook shows up as a harness gap instead of passing silently.

Latency: replayed queries sleep the visual's latency from synthetic_capture.VISUALS (A or B), x1.8 the first time a
model runs a visual (cold cache), x(1 + 0.15 per other query running on the same model), x lognormal noise (sigma 0.12),
x scale. Metadata / number-check queries sleep ~5 ms. Active traces record QueryEnd + storage-engine events (with the
connection's SessionID) plus some events of a foreign session, which the notebook must ignore.
"""
import math
import os
import random
import re
import shutil
import sys
import threading
import time
import types
import urllib.parse
import uuid
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from synthetic_capture import TRUTH, VISUAL_BY_ID

XMLA_PREFIX = "powerbi://api.powerbi.com/v1.0/myorg/"


class NotebookExit(Exception):
    """Raised by notebookutils.notebook.exit(value); the harness catches it."""

    def __init__(self, value):
        super().__init__(str(value)[:200])
        self.value = value


class FakeDaxError(Exception):
    """An engine error; .Message mimics the .NET exception property the notebook reads first."""

    def __init__(self, message):
        super().__init__(message)
        self.Message = message


def _utc_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------- the synthetic model -------------------------------------------------------------------------------------
BU = [("BU01", "North"), ("BU02", "South"), ("BU03", "East"), ("BU04", "West"), ("BU05", "Central"), ("BU06", "Coastal"),
      ("BU07", "Mountain"), ("BU08", "Lakes"), ("BU09", "Metro"), ("BU10", "Rural"), ("BU11", "Harbor"), ("BU12", "Valley")]
HISTORY_DAYS = 120
MEASURE_EXPR = {
    "Total Sales": "SUM('Sales'[Amount])",
    "Total Quantity": "SUM('Sales'[Quantity])",
    "Total Cost": "SUM('Sales'[Cost])",
    "Order Lines": "COUNTROWS('Sales')",
    "Margin": "[Total Sales] - [Total Cost]",
    "Margin %": "DIVIDE([Margin], [Total Sales])",
    "Avg Price": "DIVIDE([Total Sales], [Total Quantity])",
    "Units per Line": "DIVIDE([Total Quantity], [Order Lines])",
}
CHANGED_EXPR = {TRUTH["changed_measure"]: "[Total Sales] - [Total Cost] * 1.02"}   # model B only
# (table lower, column lower) -> column of the Sales frame (dimension columns are joined into it)
COLMAP = {("date", "date"): "Date", ("date", "month"): "Month", ("sales", "date"): "Date",
          ("business unit", "business unit"): "BU", ("business unit", "bu code"): "BUCode", ("sales", "bu code"): "BUCode",
          ("sales", "amount"): "Amount", ("sales", "quantity"): "Quantity", ("sales", "cost"): "Cost"}


def _div(a, b):
    return None if (a is None or b is None or b == 0) else a / b


def _measures(changed):
    def total(col, as_int=False):
        def f(d):
            if not len(d):
                return None
            s = d[col].sum()
            return int(s) if as_int else float(s)
        return f
    ts, tq, tc = total("Amount"), total("Quantity", True), total("Cost")
    lines = lambda d: int(len(d)) if len(d) else None                                    # noqa: E731
    if changed:
        margin = lambda d: None if ts(d) is None else ts(d) - tc(d) * 1.02               # noqa: E731
    else:
        margin = lambda d: None if ts(d) is None else ts(d) - tc(d)                      # noqa: E731
    return {"total sales": ts, "total quantity": tq, "total cost": tc, "order lines": lines, "margin": margin,
            "margin %": lambda d: _div(margin(d), ts(d)), "avg price": lambda d: _div(ts(d), tq(d)),
            "units per line": lambda d: _div(tq(d), lines(d))}


def _base_sales(last_day):
    """Deterministic Sales rows from HISTORY_DAYS before last_day up to last_day (same rows in both models)."""
    rng, rows = random.Random(20261023), []
    first = last_day - timedelta(days=HISTORY_DAYS - 1)
    for n in range(HISTORY_DAYS):
        d = first + timedelta(days=n)
        for code, name in BU:
            for _ in range(2 + rng.randrange(3)):
                amount = round(50 + rng.random() * 950, 2)
                rows.append({"Date": d, "Month": d.strftime("%Y-%m"), "BU": name, "BUCode": code, "Amount": amount,
                             "Quantity": 1 + rng.randrange(9), "Cost": round(amount * (0.5 + rng.random() * 0.2), 2)})
    return pd.DataFrame(rows)


class FakeModel:
    def __init__(self, key, workspace, model, base, now):
        self.key, self.workspace, self.model = key, workspace, model
        lag = TRUTH["a_data_lag_days"] if key == "A" else TRUTH["b_data_lag_days"]
        self.last_day = now.date() - timedelta(days=lag)
        self.refreshed = (now - timedelta(hours=6)) if key == "A" else datetime.combine(
            self.last_day + timedelta(days=1), datetime.min.time()) + timedelta(hours=4)
        self.sales = base[base["Date"] <= self.last_day].reset_index(drop=True)
        if key == "B":   # late-arriving data: the newest days of the snapshot miss the last row per business unit and day
            recent = self.sales["Date"] > self.last_day - timedelta(days=TRUTH["b_unsettled_days"])
            last_row = ~self.sales.duplicated(["Date", "BU"], keep="last")
            self.sales = self.sales[~(recent & last_row)].reset_index(drop=True)
        first = base["Date"].min()
        self.dates = [first + timedelta(days=i) for i in range((date(now.year, 12, 31) - first).days + 1)]
        self.measure_fn = _measures(changed=(key == "B"))
        exprs = dict(MEASURE_EXPR, **(CHANGED_EXPR if key == "B" else {}))
        self.measures = exprs
        self.info = self._catalog(exprs)

    # INFO.* tables (column names as the engine returns them)
    def _catalog(self, exprs):
        tables = [(1, "Sales", None), (2, "Date", "Time"), (3, "Business Unit", None)]
        T = [{"ID": i, "Name": n, "DataCategory": c, "IsHidden": False, "CalculationGroupID": None,
              "Description": None} for i, n, c in tables]
        cols, cid = [], [100]

        def col(tid, name, dt, key=False, unique=False, hidden=False, typ=1, expr=None):
            cid[0] += 1
            cols.append({"ID": cid[0], "TableID": tid, "ExplicitName": name if typ != 3 else None,
                         "InferredName": name, "ExplicitDataType": dt, "InferredDataType": dt, "IsKey": key,
                         "IsUnique": unique, "IsHidden": hidden, "Type": typ, "Expression": expr})
            return cid[0]
        for tid in (1, 2, 3):
            col(tid, f"RowNumber-2662979B-1795-4F74-8F37-6A1BA8059B6{tid}", 6, typ=3, hidden=True)
        s_date = col(1, "Date", 9)
        s_bu = col(1, "BU Code", 2, hidden=True)
        col(1, "Amount", 8)
        col(1, "Quantity", 6)
        col(1, "Cost", 8)
        col(1, "Line Margin", 8, typ=2, expr="'Sales'[Amount] - 'Sales'[Cost]")
        d_date = col(2, "Date", 9, key=True, unique=True)
        col(2, "Month", 2)
        col(3, "Business Unit", 2)
        b_code = col(3, "BU Code", 2, unique=True)
        rels = [{"ID": 1, "FromColumnID": s_date, "ToColumnID": d_date, "IsActive": True},
                {"ID": 2, "FromColumnID": s_bu, "ToColumnID": b_code, "IsActive": True}]
        m_def = 'let\n    Source = Sql.Database(SqlServer, "sales")\nin\n    Source'
        parts = [{"ID": 10 + i, "TableID": i, "Name": n, "Mode": 0, "Type": 4, "QueryDefinition": m_def.replace("sales", n),
                  "RefreshedTime": self.refreshed} for i, n, _ in tables]
        meas = [{"ID": 200 + i, "TableID": 1, "Name": n, "Expression": e} for i, (n, e) in enumerate(exprs.items())]
        calcdep = []   # INFO.CALCDEPENDENCY rows of the measures, read from their expressions
        for n, e in exprs.items():
            refs = []
            for t, c in re.findall(r"'([^']+)'\[([^\]]+)\]", e):
                refs.append(("COLUMN", t, c))
            for m in re.findall(r"\[([^\]]+)\]", re.sub(r"'[^']+'\[[^\]]+\]", "", e)):
                refs.append(("MEASURE", "Sales", m))
            for t in re.findall(r"COUNTROWS\('([^']+)'\)", e):
                refs.append(("TABLE", None, t))
            for kind, rt, ro in refs:
                calcdep.append({"DATABASE_NAME": self.model, "OBJECT_TYPE": "MEASURE", "TABLE": "Sales", "OBJECT": n,
                                "EXPRESSION": e, "REFERENCED_OBJECT_TYPE": kind, "REFERENCED_TABLE": rt,
                                "REFERENCED_OBJECT": ro, "REFERENCED_EXPRESSION": None})
        return {
            "TABLES": T, "COLUMNS": cols, "MEASURES": meas, "RELATIONSHIPS": rels, "PARTITIONS": parts,
            "ROLES": [{"ID": 1, "Name": "Reader"}], "CALCULATIONGROUPS": [],
            "CALCULATIONITEMS": [], "EXPRESSIONS": [{"ID": 1, "Name": "SqlServer",
                                                     "Expression": '"sql-sales" meta [IsParameterQuery=true]'}],
            "CALCDEPENDENCY": calcdep,
        }

    def info_columns(self, name):
        known = {"CALCULATIONITEMS": ["ID", "CalculationGroupID", "Name", "Expression"],
                 "CALCULATIONGROUPS": ["ID", "TableID", "Precedence"]}
        rows = self.info[name]
        return list(rows[0].keys()) if rows else known.get(name, [])

    def table_rows(self, table):
        t = table.lower()
        if t == "sales":
            return len(self.sales)
        if t == "date":
            return len(self.dates)
        if t == "business unit":
            return len(BU)
        raise FakeDaxError(f"Cannot find table '{table}'.")

    def column_values(self, table, column):
        k = (table.lower(), column.lower())
        if k == ("business unit", "business unit"):
            return [n for _, n in BU]
        if k == ("business unit", "bu code"):
            return [c for c, _ in BU]
        if k == ("date", "date"):
            return list(self.dates)
        if k == ("date", "month"):
            return sorted({d.strftime("%Y-%m") for d in self.dates})
        if k in COLMAP:
            return list(self.sales[COLMAP[k]].unique())
        raise FakeDaxError(f"Column '{column}' in table '{table}' cannot be found.")


# ---------- a tiny reader for the DAX templates the notebook sends --------------------------------------------------
_COL = r"(?:'(?:[^']|'')+'|[A-Za-z_][\w ]*?)\[(?:[^\]]|\]\])+\]"


def _token_end(s, i):
    """End index (exclusive) of the quoted token that starts at s[i] ('...', "..." or [...], doubled closer = escape),
    or None when s[i] doesn't start one."""
    close = {"'": "'", '"': '"', "[": "]"}.get(s[i])
    if close is None:
        return None
    j = i + 1
    while j < len(s):
        if s[j] == close:
            if j + 1 < len(s) and s[j + 1] == close:
                j += 2
                continue
            return j + 1
        j += 1
    return len(s)


def split_args(s):
    """Split at top-level commas (respecting (), {}, [], '...' and "...")."""
    out, depth, start, i = [], 0, 0, 0
    while i < len(s):
        end = _token_end(s, i)
        if end is not None:
            i = end
            continue
        ch = s[i]
        if ch in "({":
            depth += 1
        elif ch in ")}":
            depth -= 1
        elif ch == "," and depth == 0:
            out.append(s[start:i].strip())
            start = i + 1
        i += 1
    if s[start:].strip():
        out.append(s[start:].strip())
    return out


def call_body(text, fname):
    """The text between the parentheses of the first fname( ... ) call, or None."""
    m = re.search(re.escape(fname) + r"\s*\(", text)
    if not m:
        return None
    depth, i = 1, m.end()
    while i < len(text):
        end = _token_end(text, i)
        if end is not None:
            i = end
            continue
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return text[m.end():i]
        i += 1
    return None


def parse_col(text):
    m = re.fullmatch(r"\s*(?:'((?:[^']|'')+)'|([A-Za-z_][\w ]*?))\s*\[((?:[^\]]|\]\])+)\]\s*", text)
    if not m:
        return None
    return (m.group(1).replace("''", "'") if m.group(1) else m.group(2)), m.group(3).replace("]]", "]")


def _lit(v):
    v = v.strip()
    if v.startswith('"'):
        return v[1:-1].replace('""', '"')
    if v.upper() in ("TRUE()", "FALSE()"):
        return v.upper() == "TRUE()"
    try:
        return int(v)
    except ValueError:
        return float(v)


def _dax_date(y, m, d):
    return date(int(y), int(m), int(d))


def _out(v):
    """Engine value -> what ADOMD.NET would hand to Python (dates as datetime, numpy -> Python)."""
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return datetime(v.year, v.month, v.day)
    if hasattr(v, "item"):
        v = v.item()
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


class DaxAnswerer:
    def __init__(self, model):
        self.m = model

    def answer(self, text):
        t = "\n".join(ln for ln in text.replace("\r\n", "\n").split("\n") if not ln.strip().startswith("//")).strip()
        if re.fullmatch(r"EVALUATE\s*\{\s*1\s*\}", t):
            return ["[Value]"], [[1]]
        if "SUMMARIZECOLUMNS(" in t:
            return self.summarize(t)
        m = re.fullmatch(r"EVALUATE\s+ROW\((.*)\)", t, re.S)
        if m:
            return self.row(m.group(1))
        m = re.fullmatch(r"EVALUATE\s+SELECTCOLUMNS\((.*)\)", t, re.S)
        if m:
            return self.selectcolumns(m.group(1))
        m = re.fullmatch(r"EVALUATE\s+(FILTER\(INFO\.\w+\(\),.*\))", t, re.S)
        if m:
            name, rows = self.info_source(m.group(1))
            cols = self.m.info_columns(name)
            return [f"[{c}]" for c in cols], [[_out(r.get(c)) for c in cols] for r in rows]
        raise FakeDaxError(f"unsupported query (fake engine): {t[:300]}")

    # ---- INFO.* ----
    def info_source(self, src):
        src = src.strip()
        m = re.fullmatch(r"INFO\.(\w+)\(\)", src)
        if m:
            return m.group(1), list(self.m.info[m.group(1)])
        m = re.fullmatch(r"FILTER\(INFO\.(\w+)\(\),\s*\[(\w+)\]\s*(=|<>)\s*(.+)\)", src, re.S)
        if m:
            name, col, op, val = m.group(1), m.group(2), m.group(3), _lit(m.group(4))
            rows = [r for r in self.m.info[name] if (r.get(col) == val) == (op == "=")]
            return name, rows
        raise FakeDaxError(f"unsupported INFO source (fake engine): {src[:200]}")

    def info_expr(self, row, e):
        e = e.strip()
        m = re.fullmatch(r"\[(\w+)\]", e)
        if m:
            return row.get(m.group(1))
        if e.upper() in ("TRUE()", "FALSE()"):
            return e.upper() == "TRUE()"
        body = call_body(e, "IF") if e.upper().startswith("IF(") else None
        if body is not None:
            cond, a, b = split_args(body)
            mc = re.fullmatch(r"LEN\(\[(\w+)\]\)\s*>\s*0", cond)
            if mc:
                ok = len(str(row.get(mc.group(1)) or "")) > 0
            else:
                mc = re.fullmatch(r"\[(\w+)\]\s*(=|<>)\s*(.+)", cond)
                if not mc:
                    raise FakeDaxError(f"unsupported IF condition (fake engine): {cond}")
                ok = (row.get(mc.group(1)) == _lit(mc.group(3))) == (mc.group(2) == "=")
            return self.info_expr(row, a if ok else b)
        raise FakeDaxError(f"unsupported expression (fake engine): {e[:200]}")

    def selectcolumns(self, body):
        args = split_args(body)
        _, rows = self.info_source(args[0])
        pairs = [(_lit(args[i]), args[i + 1]) for i in range(1, len(args), 2)]
        return [f"[{a}]" for a, _ in pairs], [[_out(self.info_expr(r, e)) for _, e in pairs] for r in rows]

    # ---- ROW(...) ----
    def row(self, body):
        args = split_args(body)
        cols, vals = [], []
        for i in range(0, len(args), 2):
            cols.append(f"[{_lit(args[i])}]")
            vals.append(_out(self.scalar(args[i + 1])))
        return cols, [vals]

    def scalar(self, e):
        e = e.strip()
        m = re.fullmatch(r"COUNTROWS\(INFO\.(\w+)\(\)\)", e)
        if m:
            return len(self.m.info[m.group(1)])
        if re.fullmatch(r"MAXX\(INFO\.PARTITIONS\(\),\s*\[RefreshedTime\]\)", e):
            return max(p["RefreshedTime"] for p in self.m.info["PARTITIONS"])
        m = re.fullmatch(r"DISTINCTCOUNT\((.+)\)", e, re.S)
        if m:
            ref = parse_col(m.group(1))
            return len(set(self.m.column_values(*ref)))
        m = re.fullmatch(r"COUNTROWS\('((?:[^']|'')+)'\)", e)
        if m:
            return self.m.table_rows(m.group(1).replace("''", "'"))
        m = re.fullmatch(r"MAXX\(FILTER\(VALUES\((" + _COL + r")\),\s*(?:(" + _COL + r")\s*<\s*DATE\((\d+),\s*(\d+),\s*(\d+)\)\s*&&\s*)?"
                         r"NOT ISEMPTY\(CALCULATETABLE\('((?:[^']|'')+)'\)\)\),\s*(" + _COL + r")\)", e, re.S)
        if m:
            table = m.group(6).replace("''", "'")
            if table.lower() != "sales":
                return None
            d = self.m.sales["Date"]
            if m.group(3):
                d = d[d < _dax_date(m.group(3), m.group(4), m.group(5))]
            return d.max() if len(d) else None
        raise FakeDaxError(f"unsupported scalar (fake engine): {e[:200]}")

    # ---- SUMMARIZECOLUMNS (number check and replayed report queries) ----
    def summarize(self, t):
        df = self.m.sales
        filters = {}
        for name, vals, col in re.findall(r"VAR\s+(\w+)\s*=\s*TREATAS\(\{(.*?)\},\s*(" + _COL + r")\)", t, re.S):
            filters[name] = ("in", parse_col(col), [_lit(v) for v in split_args(vals)])
        for m in re.finditer(r"VAR\s+(\w+)\s*=\s*FILTER\(KEEPFILTERS\(VALUES\((" + _COL + r")\)\),\s*AND\(.*?>=\s*DATE\((\d+),\s*(\d+),"
                             r"\s*(\d+)\),\s*.*?<\s*DATE\((\d+),\s*(\d+),\s*(\d+)\)\)\)", t, re.S):
            filters[m.group(1)] = ("range", parse_col(m.group(2)), (_dax_date(*m.group(3, 4, 5)), _dax_date(*m.group(6, 7, 8))))
        args = split_args(call_body(t, "SUMMARIZECOLUMNS"))
        groups, rollup, pairs, i = [], None, [], 0
        while i < len(args):
            a = args[i]
            if a.upper().startswith("ROLLUPADDISSUBTOTAL("):
                inner = split_args(call_body(a, "ROLLUPADDISSUBTOTAL"))
                groups.append(parse_col(inner[0]))
                rollup = (len(groups) - 1, _lit(inner[1]))
            elif a.startswith('"'):
                pairs.append((_lit(a), args[i + 1]))
                i += 1
            elif re.fullmatch(r"__\w+", a):
                kind, ref, val = filters[a]
                c = COLMAP[(ref[0].lower(), ref[1].lower())]
                df = df[df[c].isin(val)] if kind == "in" else df[(df[c] >= val[0]) & (df[c] < val[1])]
            else:
                ref = parse_col(a)
                if ref is None:
                    raise FakeDaxError(f"unsupported SUMMARIZECOLUMNS argument (fake engine): {a[:200]}")
                groups.append(ref)
            i += 1
        fns = [self.measure(e) for _, e in pairs]
        gcols = [COLMAP[(t_.lower(), c.lower())] for t_, c in groups]
        cols = ([f"{t_}[{c}]" for t_, c in groups] + ([f"[{rollup[1]}]"] if rollup else [])
                + [f"[{a}]" for a, _ in pairs])
        rows = []

        def emit(keys, flag, part):
            vals = [f(part) for f in fns]
            if all(v is None for v in vals):
                return
            rows.append([_out(k) for k in keys] + ([flag] if rollup else []) + [_out(v) for v in vals])
        if not gcols:
            emit([], None, df)
            return cols, rows
        for keys, part in df.groupby(gcols, sort=True):
            keys = list(keys) if isinstance(keys, tuple) else [keys]
            emit(keys, False, part)
        if rollup:
            ri = rollup[0]
            rest = [c for j, c in enumerate(gcols) if j != ri]
            if rest:
                for keys, part in df.groupby(rest, sort=True):
                    keys = list(keys) if isinstance(keys, tuple) else [keys]
                    full = keys[:ri] + [None] + keys[ri:]
                    emit(full, True, part)
            else:
                emit([None], True, df)
        return cols, rows

    def measure(self, e):
        e = e.strip()
        m = re.fullmatch(r"COUNTROWS\('((?:[^']|'')+)'\)", e)
        if m:
            t = m.group(1).replace("''", "'").lower()
            if t == "sales":
                return lambda d: int(len(d)) if len(d) else None
            if t == "date":
                return lambda d: int(d["Date"].nunique()) if len(d) else None
            if t == "business unit":
                return lambda d: int(d["BU"].nunique()) if len(d) else None
            raise FakeDaxError(f"Cannot find table '{m.group(1)}'.")
        m = re.fullmatch(r"(?:'(?:[^']|'')+')?\[((?:[^\]]|\]\])+)\]", e)
        if m:
            name = m.group(1).replace("]]", "]").lower()
            if name not in self.m.measure_fn:
                raise FakeDaxError(f"The value for '{m.group(1)}' cannot be determined.")
            return self.m.measure_fn[name]
        raise FakeDaxError(f"unsupported measure expression (fake engine): {e[:200]}")


# ---------- the engine, connections and traces ---------------------------------------------------------------------
class FakeEngine:
    def __init__(self, models, scale=1.0, seed=7, capacity="CAP-0001-FAKE", options=None):
        """models: [(key, workspace, model)] for A and B. options (scenario injections), all optional:
        drop: ["B"] = that model can't be found (e.g. the branch workspace was deleted);
        drift: {"model": "B", "from": i, "to": j, "factor": f} = replayed queries i..j-1 of that model are f times slower
               (counted per model: warm-up, trace, then the load rounds), i.e. one round slowed by something else;
        errors: {"B": {n: "message"}} = the n-th replayed query of that model fails with this engine message;
        top_load: {"B": (threshold, k)} = extra x(1 + k * (busy - threshold)) when more than threshold queries run
                  on that model at once (slower only at the top concurrency level)."""
        now = _utc_naive()
        base = _base_sales(now.date() - timedelta(days=TRUTH["a_data_lag_days"]))
        self.models = {(ws, m): FakeModel(k, ws, m, base, now) for k, ws, m in models}
        self.answerers = {k: DaxAnswerer(v) for k, v in self.models.items()}
        self.scale, self.rng, self.lock = float(scale), random.Random(seed), threading.Lock()
        self.active, self.seen, self.traces = {}, set(), []
        self.capacity = capacity
        self.stats = {"queries": 0, "replayed": 0, "errors": 0, "connections": 0}
        self.unsupported = []
        self.options = dict(options or {})
        self.replay_no = {"A": 0, "B": 0}

    def find(self, workspace, model):
        hit = self.models.get((workspace, model))
        if hit is not None and hit.key in self.options.get("drop", []):
            hit = None
        if hit is None:
            raise FakeDaxError(f"The database '{model}' couldn't be found in workspace '{workspace}' (fake engine).")
        return hit

    def _sleep(self, ms):
        if ms > 0:
            time.sleep(ms * self.scale / 1000.0)

    def execute(self, conn, text):
        mdl = conn.model
        key = (mdl.workspace, mdl.model)
        vis_m = re.search(r"//\s*vis:(\w+)", text or "")
        vis = VISUAL_BY_ID.get(vis_m.group(1)) if vis_m else None
        with self.lock:
            self.stats["queries"] += 1
            self.active[key] = self.active.get(key, 0) + 1
            busy = self.active[key]
            cold = vis is not None and (key, vis["id"]) not in self.seen
            n_replay = None
            if vis is not None:
                self.seen.add((key, vis["id"]))
                self.stats["replayed"] += 1
                n_replay = self.replay_no[mdl.key]
                self.replay_no[mdl.key] += 1
            noise = self.rng.lognormvariate(0, 0.12)
        start = _utc_naive()
        try:
            cols, rows = self.answerers[key].answer(text)
            error = None
        except FakeDaxError as exc:
            cols, rows, error = [], [], exc
            if "fake engine" in str(exc):
                with self.lock:
                    self.unsupported.append(str(exc)[:300])
        idx = 0 if mdl.key == "A" else 1
        ms = (vis["ms"][idx] * (1.8 if cold else 1.0) * (1 + 0.15 * (busy - 1)) * noise) if vis else 5 * noise
        if vis is not None:
            dr = self.options.get("drift")
            if dr and dr["model"] == mdl.key and dr["from"] <= n_replay < dr["to"]:
                ms *= dr["factor"]
            top = self.options.get("top_load", {}).get(mdl.key)
            if top and busy > top[0]:
                ms *= 1 + top[1] * (busy - top[0])
            msg = self.options.get("errors", {}).get(mdl.key, {}).get(n_replay)
            if msg and error is None:
                error = FakeDaxError(msg)
        self._sleep(ms)
        with self.lock:
            self.active[key] -= 1
        end = _utc_naive()
        self._log(conn, mdl, text, start, end, vis, idx, error is None)
        if error is not None:
            with self.lock:
                self.stats["errors"] += 1
            raise error
        return cols, rows

    def _log(self, conn, mdl, text, start, end, vis, idx, ok):
        traces = [t for t in self.traces if t.running and t.workspace == mdl.workspace and t.model == mdl.model]
        if not traces:
            return
        dur = max(int((end - start).total_seconds() * 1000), 1)
        cpu = int(dur * (vis["cpu"][idx] if vis else 1.0))
        ev = [{"Event Class": "QueryEnd", "Event Subclass": "DAXQuery", "Current Time": end, "Text Data": text,
               "Session ID": conn.SessionID, "Application Name": conn.app, "Start Time": start, "End Time": end,
               "Duration": dur, "Cpu Time": cpu, "Success": "1" if ok else "0"}]
        if vis:
            n = 2 + int(vis["id"][1:]) % 3
            step = (end - start) / (n + 2)
            for j in range(n):
                s = start + step * (j + 1)
                ev.append({"Event Class": "VertiPaqSEQueryEnd", "Event Subclass": "VertiPaqScan", "Current Time": s + step,
                           "Text Data": f"SELECT ... scan {j}", "Session ID": conn.SessionID, "Start Time": s,
                           "End Time": s + step, "Duration": max(int(step.total_seconds() * 1000), 1),
                           "Cpu Time": int(cpu * 0.8 / n), "Success": "1"})
            ev.append({"Event Class": "VertiPaqSEQueryEnd", "Event Subclass": "VertiPaqScanInternal", "Current Time": end,
                       "Text Data": "internal", "Session ID": conn.SessionID, "Start Time": start + step,
                       "End Time": start + step * 2, "Duration": 1, "Cpu Time": 1, "Success": "1"})
            if int(vis["id"][1:]) % 2 == 0:
                ev.append({"Event Class": "VertiPaqSEQueryCacheMatch", "Event Subclass": "VertiPaqCacheExactMatch",
                           "Current Time": start + step / 2, "Text Data": "cache", "Session ID": conn.SessionID})
            # another user's query on the same model at the same time: the notebook must not count it
            ev.append({"Event Class": "QueryEnd", "Event Subclass": "DAXQuery", "Current Time": end,
                       "Text Data": text if int(vis["id"][1:]) % 4 == 0 else "EVALUATE {2}",
                       "Session ID": "FOREIGN-SESSION-0001", "Application Name": "PowerBI", "Start Time": start,
                       "End Time": end, "Duration": dur * 3, "Cpu Time": cpu * 3, "Success": "1"})
        with self.lock:
            for t in traces:
                t.events += ev


class _Reader:
    def __init__(self, cols, rows):
        self._cols, self._rows, self._i = cols, rows, -1
        self.FieldCount = len(cols)

    def GetName(self, i):
        return self._cols[i]

    def GetValue(self, i):
        return self._rows[self._i][i]

    def Read(self):
        self._i += 1
        return self._i < len(self._rows)

    def Close(self):
        pass


class _Command:
    def __init__(self, conn):
        self._conn, self.CommandText, self.CommandTimeout = conn, "", 0

    def ExecuteReader(self):
        if not self._conn.is_open:
            raise FakeDaxError("The connection is not open.")
        return _Reader(*self._conn.engine.execute(self._conn, self.CommandText))


def _parse_cs(cs):
    out, i, key = {}, 0, ""
    while i < len(cs):
        j = cs.find("=", i)
        if j < 0:
            break
        key = cs[i:j].strip().lower()
        i = j + 1
        if i < len(cs) and cs[i] == '"':
            val, i = [], i + 1
            while i < len(cs):
                if cs[i] == '"' and i + 1 < len(cs) and cs[i + 1] == '"':
                    val.append('"')
                    i += 2
                elif cs[i] == '"':
                    i += 1
                    break
                else:
                    val.append(cs[i])
                    i += 1
            out[key] = "".join(val)
            i = cs.find(";", i) + 1 if cs.find(";", i) >= 0 else len(cs)
        else:
            k = cs.find(";", i)
            out[key] = cs[i:k if k >= 0 else len(cs)]
            i = k + 1 if k >= 0 else len(cs)
    return out


def make_adomd_connection_class(engine):
    class AdomdConnection:
        def __init__(self, connection_string):
            p = _parse_cs(connection_string)
            src = p.get("data source", "")
            self.workspace = urllib.parse.unquote(src[len(XMLA_PREFIX):]) if src.startswith(XMLA_PREFIX) else src
            self.catalog, self.app = p.get("initial catalog", ""), p.get("application name", "")
            self.engine, self.model, self.is_open = engine, None, False
            self.SessionID = str(uuid.uuid4()).upper()

        def Open(self):
            self.model = self.engine.find(self.workspace, self.catalog)
            with self.engine.lock:
                self.engine.stats["connections"] += 1
            self.engine._sleep(20)
            self.is_open = True

        def CreateCommand(self):
            return _Command(self)

        def Close(self):
            self.is_open = False
    return AdomdConnection


class FakeTrace:
    def __init__(self, engine, workspace, model):
        self.engine, self.workspace, self.model = engine, workspace, model
        self.running, self.events = False, []

    def start(self, delay=3):          # the real trace waits `delay` seconds; not needed offline
        self.running = True
        with self.engine.lock:
            self.engine.traces.append(self)

    def stop(self, timeout=5):
        self.running = False
        return pd.DataFrame(self.events)

    def drop(self):
        self.running = False
        with self.engine.lock:
            if self in self.engine.traces:
                self.engine.traces.remove(self)


class FakeTraceConnection:
    def __init__(self, engine, model, workspace):
        engine.find(workspace, model)
        self.engine, self.model, self.workspace = engine, model, workspace

    def create_trace(self, schema, name=None):
        return FakeTrace(self.engine, self.workspace, self.model)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _guid(text):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "harness:" + str(text)))


def install(engine, lakehouse_dir, host_workspace="Harness Host", context=None):
    """Put the stubs into sys.modules. Returns the notebookutils object (the cells use it as a global)."""
    lakehouse_dir = os.path.abspath(lakehouse_dir)
    host_id = _guid(host_workspace)

    # ---- System / ADOMD.NET ----
    system = types.ModuleType("System")
    system.Convert = types.SimpleNamespace(ToDouble=float)
    glob = types.ModuleType("System.Globalization")
    glob.CultureInfo = types.SimpleNamespace(InvariantCulture="InvariantCulture")
    system.Globalization = glob
    ms = types.ModuleType("Microsoft")
    asv = types.ModuleType("Microsoft.AnalysisServices")
    adomd = types.ModuleType("Microsoft.AnalysisServices.AdomdClient")
    adomd.AdomdConnection = make_adomd_connection_class(engine)
    ms.AnalysisServices, asv.AdomdClient = asv, adomd

    # ---- sempy.fabric ----
    sempy = types.ModuleType("sempy")
    fabric = types.ModuleType("sempy.fabric")
    sempy.fabric = fabric
    fabric.create_tom_server = lambda readonly=True, workspace=None: types.SimpleNamespace(readonly=readonly)
    fabric.resolve_workspace_id = lambda name=None: _guid(name)
    fabric.get_workspace_id = lambda: host_id
    fabric.resolve_item_id = lambda name, type=None, workspace=None: _guid(f"{workspace}/{type}/{name}")
    wss = sorted({ws for ws, _ in engine.models} | {host_workspace})
    fabric.list_workspaces = lambda *a, **k: pd.DataFrame(
        [{"Id": _guid(w), "Name": w, "Type": "Workspace", "Capacity Id": engine.capacity} for w in wss])
    fabric.create_trace_connection = lambda dataset, workspace=None: FakeTraceConnection(engine, dataset, workspace)

    class Trace:
        @staticmethod
        def get_default_query_trace_schema():
            cols = ["EventClass", "EventSubclass", "CurrentTime", "TextData", "SessionID", "ApplicationName",
                    "StartTime", "EndTime", "Duration", "CpuTime", "Success"]
            return {"QueryBegin": cols, "QueryEnd": cols, "VertiPaqSEQueryBegin": cols, "VertiPaqSEQueryEnd": cols,
                    "VertiPaqSEQueryCacheMatch": cols[:6]}
    fabric.Trace = Trace

    # ---- notebookutils ----
    def _local(path):   # file://..., abfss://<ws>@onelake.../<lakehouse>/<rel> or a local path -> local path
        if path.startswith("file://"):
            return path[len("file://"):]
        if path.startswith("abfss://"):
            rel = path.split("/", 4)[4] if path.count("/") >= 4 else ""
            return os.path.join(lakehouse_dir, *rel.split("/"))
        return path

    def cp(src, dst, recurse=False):
        src, dst = _local(src), _local(dst)
        if not os.path.exists(src):
            raise FileNotFoundError(f"Operation failed: 'Not Found', 404, path {src}")
        os.makedirs(os.path.dirname(os.path.abspath(dst)), exist_ok=True)   # OneLake creates folders on write
        shutil.copyfile(src, dst)
        return True

    def exit_(value=None):
        raise NotebookExit(value)
    ctx = {"currentWorkspaceId": host_id, "currentWorkspaceName": host_workspace,
           "currentNotebookName": "ModelCompare_LoadTest", "currentNotebookId": _guid("notebook"),
           "isForPipeline": False, "isReferenceRun": False, "currentRunId": None, "productType": "Fabric"}
    ctx.update(context or {})
    nbu = types.SimpleNamespace(
        runtime=types.SimpleNamespace(context=ctx),
        credentials=types.SimpleNamespace(getToken=lambda audience: f"fake-token-{audience}"),
        fs=types.SimpleNamespace(cp=cp, exists=lambda p: os.path.exists(p)),
        notebook=types.SimpleNamespace(exit=exit_))
    nbu_mod = types.ModuleType("notebookutils")
    for k, v in vars(nbu).items():
        setattr(nbu_mod, k, v)

    sys.modules.update({"System": system, "System.Globalization": glob, "Microsoft": ms,
                        "Microsoft.AnalysisServices": asv, "Microsoft.AnalysisServices.AdomdClient": adomd,
                        "sempy": sempy, "sempy.fabric": fabric, "notebookutils": nbu_mod})
    return nbu_mod
