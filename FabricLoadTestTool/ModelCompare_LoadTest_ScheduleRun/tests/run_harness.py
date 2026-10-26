"""Offline harness: runs ModelCompare_LoadTest.py cell by cell against the fakes (tests/fakes.py), like "Run all" in
Fabric, and checks the outputs against the injected truth (tests/synthetic_capture.py).

    python tests/run_harness.py --all --out <folder>                 # build check + every scenario
    python tests/run_harness.py --scenario confirmed --out <folder>  # one scenario (some need an earlier one: see ORDER)

Per scenario it writes <out>/<scenario>/:
    steps.txt        everything each step printed, step by step (display() outputs as text)
    report.html      what the notebook shows, in order: printed text, tables, HTML and charts (embedded images)
    assertions.txt   the checks and their result (+ NOTE lines: observations);  result.json  the same, machine-readable
    lakehouse/       the local lakehouse (real Delta tables and report files written by the notebook)
Needs pandas, numpy, pyarrow, deltalake, matplotlib, ipython (a venv outside the repo). Exit code 1 = a check failed.

How a scenario runs: the cells are split with build_notebook.parse_py and executed in one namespace. After the
parameter cell the scenario's overrides are applied (like the parameter cell Fabric injects for a pipeline run).
After Step 2 LAKEHOUSE_ROOT points at the local lakehouse folder, and a scenario may seed rows with the notebook's own
write_table (e.g. a run that is still 'running'). Nothing in the notebook source is changed.
reopen_legacy needs a lakehouse saved by an older notebook version (--legacy DIR, default <out>/../fixtures/r1_confirmed
with lakehouse/ and result.json); it is reported as not run when that folder is missing.
"""
import argparse
import base64
import contextlib
import html
import io
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import traceback
import _thread
from datetime import datetime, timedelta, timezone

sys.dont_write_bytecode = True   # no __pycache__ in the repo
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"   # ... nor from the scenario subprocesses
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
NOTEBOOK_PY = os.path.join(ROOT, "ModelCompare_LoadTest.py")
TABLES = ["lt_comparison", "lt_query_catalog", "lt_model_profile", "lt_executions", "lt_server_metrics", "lt_result_compare",
          "lt_run_summary"]
QUERY_FILE = "Files/LoadTest/Queries/PowerBIPerformanceData.json"
REPORTS = "Files/LoadTest/Reports"
PIPELINE = {"isForPipeline": True}   # what a pipeline / scheduled job run looks like in notebookutils.runtime.context
THROTTLE_MSG = "Too many requests: the capacity is overloaded and the request was throttled. Please retry later."
TIMEOUT_MSG = "The operation was cancelled because the query timeout of 600 seconds was exceeded."

BASE_OVERRIDES = {   # fast and deterministic; everything else keeps the notebook's defaults
    "test_name": "Harness - Prod vs Branch",
    "a_label": "Prod", "a_workspace": "Harness Prod", "a_model": "SalesModel",
    "b_label": "Branch", "b_workspace": "Harness Branch [1234]", "b_model": "SalesModel",
    "concurrency_levels": "1,2", "iterations": 2, "think_time_between_actions_sec": 0,
    "number_check_seed": 4242,
}


def _utc_hhmm(minutes):
    return f"{datetime.now(timezone.utc) + timedelta(minutes=minutes):%H:%M}"


SCENARIOS = {
    "dry_run": {"overrides": {"confirm_run": False},
                "about": "confirm_run = False: preflight + number check + approval summary; nothing may be written"},
    "confirmed": {"overrides": {"confirm_run": True},
                  "about": "confirm_run = True: the full test, then report, history and text summary"},
    "reopen_by_id": {"overrides": {"confirm_run": False}, "lake_from": "confirmed", "report_from": "confirmed",
                     "steps": (1, 2, 8, 9, 10),
                     "about": "report_comparison_id = the confirmed run; Steps 1, 2, 8, 9, 10 only; nothing may be written"},
    "reopen_legacy": {"overrides": {"confirm_run": False}, "lake_from": "legacy", "report_from": "legacy",
                      "steps": (1, 2, 8, 9, 10),
                      "about": "a comparison saved by the R1 notebook (no fingerprint, no lt_run_summary) still renders; "
                               "History computes it on read and writes nothing"},
    "interrupted_load": {"overrides": {"confirm_run": True}, "interrupt_step": 7, "interrupt_after_sec": 10.5,
                         "about": "confirm_run = True, the load test is cancelled mid-round after ~10 s (KeyboardInterrupt, "
                                  "like cancelling the cell); then Steps 8-10 run as a person would to see the partial result"},
    "load_issues": {"overrides": {"confirm_run": True},
                    "engine": {"drift": {"model": "B", "from": 24, "to": 32, "factor": 1.7},
                               "errors": {"B": {20: THROTTLE_MSG, 40: TIMEOUT_MSG}}, "top_load": {"B": (3, 0.5)}},
                    "about": "confirm_run = True with injected trouble: one Branch round 1.7x slower (round drift), one "
                             "throttled and one timed-out Branch query, Branch slower only at the top level (2 users)"},
    "scheduled_bad_fingerprint": {"overrides": {"schedule_approval": "000000000000"}, "context": PIPELINE,
                                  "expect_fail": "Step 3",
                                  "about": "pipeline run, schedule_approval does not match: the run FAILS before touching "
                                           "the models, and says how to re-approve"},
    "scheduled_outside_window": {"overrides": {"schedule_window": lambda: f"{_utc_hhmm(120)}-{_utc_hhmm(180)}"},
                                 "approve": True, "context": PIPELINE,
                                 "about": "pipeline run, approved, but started outside schedule_window: skipped"},
    "scheduled_locked": {"overrides": {}, "approve": True, "context": PIPELINE, "seed": "running_row",
                         "about": "pipeline run, approved, another run of the same test is 'running' (5 min ago): skipped"},
    "scheduled_unreachable_model": {"overrides": {}, "approve": True, "context": PIPELINE, "engine": {"drop": ["B"]},
                                    "expect_fail": "Step 3",
                                    "about": "pipeline run, approved, the Branch model no longer exists: the run FAILS "
                                             "(raises, so the job alerts), with a 'failed' row; it is not a skip"},
    "scheduled_ok": {"overrides": {"run_warmup": False, "schedule_window": lambda: f"{_utc_hhmm(-60)}-{_utc_hhmm(60)}"},
                     "approve": True, "context": PIPELINE, "lake_from": "confirmed", "seed": "trend_runs",
                     "about": "pipeline run, approved, inside the window: the full test (warm-up forced on), report + "
                              "text + result files saved, exit JSON, trend line vs 5 seeded earlier runs"},
}
ORDER = ["dry_run", "confirmed", "reopen_by_id", "reopen_legacy", "interrupted_load", "load_issues",
         "scheduled_bad_fingerprint", "scheduled_outside_window", "scheduled_locked", "scheduled_unreachable_model",
         "scheduled_ok"]


# ---------- output capture ---------------------------------------------------------------------------------------
class Capture:
    """Collects one step's output in order: ("text", str) and ("html", html, text) chunks."""

    def __init__(self, echo):
        self.steps, self.cur, self.echo, self.lock = [], None, echo, threading.Lock()
        try:   # the Windows console can't print every character (e.g. "Δ"); the echo must never fail a cell
            sys.__stdout__.reconfigure(errors="replace")
        except Exception:
            self.echo = False

    def begin(self, title, kind="code", source=""):
        self.cur = {"title": title, "kind": kind, "chunks": [], "source": source, "error": None, "seconds": 0.0}
        self.steps.append(self.cur)
        if self.echo:
            sys.__stdout__.write(f"\n##### {title}\n")
            sys.__stdout__.flush()

    def write(self, s):
        if not s:
            return 0
        with self.lock:
            ch = self.cur["chunks"]
            if ch and ch[-1][0] == "text":
                ch[-1] = ("text", ch[-1][1] + s)
            else:
                ch.append(("text", s))
        if self.echo:
            sys.__stdout__.write(s)
            sys.__stdout__.flush()
        return len(s)

    def flush(self):
        pass

    def add_html(self, html_, text):
        with self.lock:
            self.cur["chunks"].append(("html", html_, text))
        if self.echo:
            sys.__stdout__.write(text + "\n")

    def text(self):
        return "\n".join(ch[1] if ch[0] == "text" else ch[2] for s in self.steps for ch in s["chunks"])


def html_to_text(h):
    h = re.sub(r"\s*\n\s*", " ", h)   # layout newlines inside the HTML (pandas Styler) are not text
    h = re.sub(r"<img[^>]*>", "[chart image]\n", h)
    h = re.sub(r"(?is)<(script|style).*?</\1>", "", h)
    h = re.sub(r"(?i)<br\s*/?>|</(tr|li|div|h\d|p|summary|ul|ol|table|details)>", "\n", h)
    h = re.sub(r"(?i)</t[dh]>", " | ", h)
    h = re.sub(r"(?i)<li[^>]*>", "- ", h)
    h = html.unescape(re.sub(r"</?[A-Za-z][^>]*>", "", h))   # only real tags: the notebook writes "(< 1 ms)" as text
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in h.split("\n")]
    return "\n".join(ln for ln in lines if ln)


def make_display(cap):
    def display(*objs, **kwargs):
        for obj in objs:
            name = type(obj).__name__
            if hasattr(obj, "_repr_html_") and name == "HTML":
                h = obj.data
                cap.add_html(h, "[display: HTML]\n" + html_to_text(h))
            elif name == "DataFrame":
                cap.add_html(obj.to_html(), f"[display: DataFrame {obj.shape[0]}x{obj.shape[1]}]\n{obj.to_string()}")
            elif name == "Styler":
                h = obj.to_html()   # the formatted values, as the notebook shows them
                cap.add_html(h, f"[display: styled DataFrame {obj.data.shape[0]}x{obj.data.shape[1]}]\n" + html_to_text(h))
            else:
                cap.add_html(f"<pre>{html.escape(repr(obj))}</pre>", f"[display: {name}] {obj!r}")
    return display


def make_show(cap, plt):
    def show(*args, **kwargs):
        for n in plt.get_fignums():
            fig = plt.figure(n)
            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=100, bbox_inches="tight")
            title = fig.axes[0].get_title() if fig.axes else ""
            plt.close(fig)
            cap.add_html(f"<img style='max-width:100%' src='data:image/png;base64,{base64.b64encode(buf.getvalue()).decode()}'>",
                         f"[chart: {title}]")
    return show


# ---------- one scenario -----------------------------------------------------------------------------------------
def _long(path):
    """Windows extended-length path (Delta part files under a deep scratch folder exceed MAX_PATH)."""
    path = os.path.abspath(path)
    prefix = "\\\\?\\"
    return prefix + path if os.name == "nt" and not path.startswith(prefix) else path


def _step_no(title):
    m = re.match(r"Step (\d+)", title)
    return int(m.group(1)) if m else None


def _load_result(out_root, name):
    with open(os.path.join(out_root, name, "result.json"), encoding="utf-8") as fh:
        return json.load(fh)


def seed(kind, ns, overrides):
    """Rows a scenario needs before Step 3, written with the notebook's own write_table (same schemas)."""
    import pandas as pd
    now = datetime.now(timezone.utc)
    base = {"test_name": overrides["test_name"], "a_label": overrides["a_label"], "a_workspace": overrides["a_workspace"],
            "a_model": overrides["a_model"], "b_label": overrides["b_label"], "b_workspace": overrides["b_workspace"],
            "b_model": overrides["b_model"], "query_file": QUERY_FILE, "query_count": 8}
    if kind == "running_row":
        ns["write_table"](pd.DataFrame([{**base, "comparison_id": "Harness-earlier-run-still-running", "status": "running",
                                         "updated_utc": now - timedelta(minutes=5), "started_utc": now - timedelta(minutes=20),
                                         "parameters_json": "{}", "message": "load test"}]), "lt_comparison")
        return "seeded one 'running' row of the same test, last update 5 min ago"
    if kind == "trend_runs":   # 5 earlier comparable scheduled runs with the same fingerprint, newer than 'confirmed'
        fp = overrides["schedule_approval"]
        p90a, p90b = [380, 420, 400, 410, 395], [430, 460, 445, 450, 440]
        comp, summ = [], []
        for i in range(5):
            cid, t = f"Harness-seeded-trend-{i + 1}", now - timedelta(seconds=50 - 8 * i)
            params = {"approval_fingerprint": fp, "run_mode": "scheduled", "concurrency_levels": "1,2", "iterations": 2,
                      "replay_mode": "page_load"}
            comp.append({**base, "comparison_id": cid, "status": "completed", "updated_utc": t, "started_utc": t,
                         "finished_utc": t, "parameters_json": json.dumps(params), "message": "seeded by the harness"})
            summ.append({"comparison_id": cid, "test_name": base["test_name"], "started_utc": t, "status": "completed",
                         "run_mode": "scheduled", "fingerprint": fp, "comparable": True, "p50_a": 170, "p50_b": 160,
                         "p90_a": p90a[i], "p90_b": p90b[i], "cpu_a": 2000, "cpu_b": 2000,
                         "metrics_json": json.dumps({"numbers": "2 differ", "not_comparable": []}), "written_utc": t})
        for i in range(2):   # then two skipped scheduled runs in a row (History shows them as one row)
            t = now - timedelta(seconds=8 - 3 * i)
            comp.append({**base, "comparison_id": f"Harness-seeded-skip-{i + 1}", "status": "skipped", "updated_utc": t,
                         "started_utc": t, "finished_utc": t, "parameters_json": json.dumps({"run_mode": "scheduled"}),
                         "message": "time window: started 03:10 UTC, outside 22:00-02:00"})
        ns["write_table"](pd.DataFrame(comp), "lt_comparison")
        ns["write_table"](pd.DataFrame(summ), "lt_run_summary")
        return ("seeded 5 earlier comparable scheduled runs (p90 Prod 380-420 ms, Branch 430-460 ms, same fingerprint) "
                "and 2 skipped runs after them")
    raise ValueError(kind)


def run_scenario(name, out_root, echo=True, scale=1.0, legacy=None):
    os.environ.setdefault("MPLBACKEND", "Agg")
    sys.path[:0] = [HERE, ROOT]
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import IPython.display
    from build_notebook import parse_py
    import fakes
    from synthetic_capture import write_capture

    sc = SCENARIOS[name]
    out = os.path.join(out_root, name)
    shutil.rmtree(_long(out), ignore_errors=True)
    lake = os.path.join(out, "lakehouse")
    overrides = dict(BASE_OVERRIDES, **{k: (v() if callable(v) else v) for k, v in sc["overrides"].items()})
    src = {"legacy": legacy}.get(sc.get("lake_from"), os.path.join(out_root, sc.get("lake_from") or ""))
    if sc.get("lake_from"):
        shutil.copytree(_long(os.path.join(src, "lakehouse")), _long(lake))   # Delta file paths exceed 260 chars
    else:
        os.makedirs(os.path.dirname(os.path.join(lake, QUERY_FILE)), exist_ok=True)
        write_capture(os.path.join(lake, QUERY_FILE))
    if sc.get("report_from"):
        rsrc = legacy if sc["report_from"] == "legacy" else os.path.join(out_root, sc["report_from"])
        with open(os.path.join(rsrc, "result.json"), encoding="utf-8") as fh:
            prev_result = json.load(fh)
        overrides["report_comparison_id"] = prev_result["comparison_id"]
    else:
        prev_result = None
    if sc.get("approve"):   # the fingerprint a person would paste after the dry run
        overrides["schedule_approval"] = _load_result(out_root, "dry_run")["fingerprint"]
    log_before = _delta_log_files(lake)

    engine = fakes.FakeEngine([("A", overrides["a_workspace"], overrides["a_model"]),
                               ("B", overrides["b_workspace"], overrides["b_model"])], scale=scale, options=sc.get("engine"))
    nbu = fakes.install(engine, lake, context=sc.get("context"))
    cap = Capture(echo)
    IPython.display.display = make_display(cap)
    plt.show = make_show(cap, plt)

    with open(NOTEBOOK_PY, "rb") as fh:
        cells = parse_py(fh.read().decode("utf-8-sig"))
    ns = {"__name__": "__main__", "notebookutils": nbu}
    steps = sc.get("steps")
    t_start, failed_cell, interrupted, exit_value = time.time(), None, False, None
    for i, (ctype, tag, title, src_) in enumerate(cells):
        step = _step_no(title)
        if steps is not None and step not in steps:
            continue
        if ctype == "markdown":
            cap.begin(title, "markdown", src_)
            continue
        cap.begin(f"CELL {i} | {title}", "code")
        timer = None
        if sc.get("interrupt_step") == step:
            timer = threading.Timer(sc["interrupt_after_sec"], _thread.interrupt_main)
        t0 = time.time()
        try:
            with contextlib.redirect_stdout(cap), contextlib.redirect_stderr(cap):
                if timer:
                    timer.start()
                exec(compile(src_, f"<cell {i}: {title}>", "exec"), ns)
                if timer:
                    timer.cancel()
                if tag == "parameters":
                    ns.update(overrides)
                    print("[harness] parameter overrides (like the parameter cell Fabric injects for a pipeline run): "
                          + ", ".join(f"{k}={v!r}" for k, v in overrides.items()))
                    if sc.get("context"):
                        print(f"[harness] Fabric runtime context: {sc['context']}")
                if title.startswith("Step 2"):
                    ns["LAKEHOUSE_ROOT"] = lake.replace(os.sep, "/")
                    print(f"[harness] LAKEHOUSE_ROOT -> {ns['LAKEHOUSE_ROOT']} (local folder; real Delta tables)")
                    if sc.get("seed"):
                        print("[harness] " + seed(sc["seed"], ns, overrides))
                        log_before = _delta_log_files(lake)
        except KeyboardInterrupt as exc:
            if timer:
                timer.cancel()
            cap.cur["error"] = "KeyboardInterrupt (harness: cancelled the cell, as planned)"
            cap.write("\n[harness] cell cancelled (KeyboardInterrupt), as planned for this scenario\n"
                      + "".join(traceback.format_exception_only(type(exc), exc)))
            interrupted = True
        except fakes.NotebookExit as exc:
            exit_value = exc.value
            cap.cur["error"] = "notebookutils.notebook.exit (as planned: the notebook ends here)"
            cap.write(f"\n[harness] notebookutils.notebook.exit({exc.value})\n")
            break
        except BaseException as exc:   # like Run all: stop at the first failing cell
            if timer:
                timer.cancel()
            planned = sc.get("expect_fail") and title.startswith(sc["expect_fail"])
            cap.cur["error"] = "".join(traceback.format_exception_only(type(exc), exc)).strip() + (
                " (as planned for this scenario)" if planned else "")
            cap.write("\n[harness] CELL FAILED" + (" (as planned)" if planned else "") + "\n" + traceback.format_exc())
            failed_cell = title
            break
        finally:
            cap.cur["seconds"] = time.time() - t0
    seconds = time.time() - t_start
    plt.close("all")

    ctx = {"ns": ns, "engine": engine, "lake": lake, "failed_cell": failed_cell, "interrupted": interrupted,
           "exit": json.loads(exit_value) if isinstance(exit_value, str) else exit_value, "log_before": log_before,
           "prev": prev_result, "steps": cap.steps, "overrides": overrides, "text": cap.text(), "out_root": out_root}
    try:
        checks = CHECKS[name](ctx)
    except Exception as exc:   # a check that crashes is a failed check, with the reason
        checks = [("checks ran", False, "".join(traceback.format_exception_only(type(exc), exc)).strip()[:300])]
    notes = NOTES.get(name, lambda c: [])(ctx)
    result = {"scenario": name, "about": sc["about"], "seconds": round(seconds, 1),
              "comparison_id": overrides.get("report_comparison_id") or ns.get("COMPARISON_ID"),
              "fingerprint": ns.get("APPROVAL_FINGERPRINT"), "exit": ctx["exit"],
              "passed": all(ok for _, ok, _ in checks), "checks": [{"check": c, "ok": ok, "detail": d} for c, ok, d in checks],
              "observations": notes, "engine": {**engine.stats, "unsupported": engine.unsupported},
              "metrics": key_metrics(ns)}
    write_outputs(out, name, sc, cap.steps, result)
    return result


# ---------- outputs ----------------------------------------------------------------------------------------------
def write_outputs(out, name, sc, steps, result):
    os.makedirs(out, exist_ok=True)
    txt = [f"HARNESS SCENARIO: {name}  ({sc['about']})", f"result: {'PASS' if result['passed'] else 'FAIL'}  "
           f"| {result['seconds']} s | comparison_id {result['comparison_id']}", ""]
    for s in steps:
        if s["kind"] == "markdown":
            continue
        txt.append("#" * 100)
        txt.append(f"# {s['title']}   ({s['seconds']:.1f} s)" + (f"   ENDED: {s['error']}" if s["error"] else ""))
        txt.append("#" * 100)
        for ch in s["chunks"]:
            txt.append(ch[1].rstrip("\n") if ch[0] == "text" else ch[2])
        txt.append("")
    with open(os.path.join(out, "steps.txt"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(txt))

    css = ("body{font-family:Segoe UI,Arial,sans-serif;margin:16px;background:#fff;color:#222}"
           "pre{white-space:pre-wrap;font-size:12px;background:#f7f7f7;padding:6px 8px;margin:4px 0;border-radius:4px}"
           ".cell{border-left:3px solid #ddd;padding-left:10px;margin:8px 0 18px 0}.md{color:#555;font-size:13px}"
           ".harness{border:2px dashed #999;padding:8px 12px;margin-bottom:16px;font-size:13px}"
           ".err{color:#b71c1c;font-weight:600}table{margin:4px 0}")
    h = [f"<!doctype html><html><head><meta charset='utf-8'><title>Harness {html.escape(name)}</title>"
         f"<style>{css}</style></head><body>"]
    rows = "".join(f"<li class='{'' if c['ok'] else 'err'}'>{'PASS' if c['ok'] else 'FAIL'}: {html.escape(c['check'])}"
                   f" <span style='color:#666'>({html.escape(str(c['detail']))})</span></li>" for c in result["checks"])
    rows += "".join(f"<li>NOTE: {html.escape(n)}</li>" for n in result.get("observations", []))
    h.append(f"<div class='harness'><b>Offline harness, scenario <code>{html.escape(name)}</code></b>: "
             f"{html.escape(sc['about'])}. Synthetic models and latencies (tests/fakes.py), not real data. "
             f"Result: <b>{'PASS' if result['passed'] else 'FAIL'}</b>, {result['seconds']} s.<ul>{rows}</ul></div>")
    for s in steps:
        if s["kind"] == "markdown":
            first, _, rest = s["source"].partition("\n")
            h.append(f"<h2 style='margin:18px 0 4px 0'>{html.escape(first.lstrip('# '))}</h2>"
                     f"<div class='md'>{html.escape(rest).replace(chr(10), '<br>')}</div>")
            continue
        h.append(f"<div class='cell'><div style='color:#999;font-size:11px'>{html.escape(s['title'])} "
                 f"({s['seconds']:.1f} s)</div>")
        for ch in s["chunks"]:
            h.append(f"<pre>{html.escape(ch[1])}</pre>" if ch[0] == "text" else ch[1])
        if s["error"]:
            h.append(f"<div class='err'>Cell ended with: {html.escape(s['error'])}</div>")
        h.append("</div>")
    h.append("</body></html>")
    with open(os.path.join(out, "report.html"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(h))
    with open(os.path.join(out, "assertions.txt"), "w", encoding="utf-8") as fh:
        fh.write(f"{name}: {'PASS' if result['passed'] else 'FAIL'} ({result['seconds']} s)\n")
        fh.writelines(f"  {'PASS' if c['ok'] else 'FAIL'}  {c['check']}  [{c['detail']}]\n" for c in result["checks"])
        fh.writelines(f"  NOTE  {n}\n" for n in result.get("observations", []))
    with open(os.path.join(out, "result.json"), "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=1, default=str)


def key_metrics(ns):
    M = ns.get("REPORT_METRICS")
    if not M:
        return {}
    pq = M["per_query"]
    A, B = M["labels"]["A"], M["labels"]["B"]
    return {"per_visual": {str(r["visual"]): {"median_a": r[f"median {A}"], "median_b": r[f"median {B}"],
                                              "change": r["change"], "cls": r["change_cls"]} for _, r in pq.iterrows()},
            "kpis": {k: {"a": v["a"], "b": v["b"], "rel": v["rel"], "cls": v["cls"]} for k, v in M["kpis"].items()},
            "status": M["status"]}


# ---------- check helpers ----------------------------------------------------------------------------------------
def _delta_log_files(lake):
    out = {}
    for t in TABLES:
        p = os.path.join(lake, "Tables", t, "_delta_log")
        out[t] = sorted(os.listdir(_long(p))) if os.path.isdir(_long(p)) else []
    return out


def _table(lake, name, cid=None):
    import pandas as pd
    from deltalake import DeltaTable
    p = os.path.join(lake, "Tables", name)
    if not os.path.isdir(_long(p)):
        return pd.DataFrame()
    df = DeltaTable(p.replace(os.sep, "/")).to_pandas()
    return df[df["comparison_id"] == cid] if cid is not None else df


def _last_status(lake, cid):
    comp = _table(lake, "lt_comparison", cid)
    return comp.sort_values("updated_utc").iloc[-1] if len(comp) else None


def _report_file(lake, cid, ext):
    p = os.path.join(lake, *REPORTS.split("/"), f"{cid}.{ext}")
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as fh:
        return fh.read()


def _cells_ok(ctx):
    bad = [s["title"] for s in ctx["steps"] if s["error"] and "as planned" not in s["error"]]
    return ("every cell ran without an unplanned error", not bad, ", ".join(bad) or "all cells ok")


def _engine_ok(ctx):
    u = ctx["engine"].unsupported
    return ("the fake engine understood every query the notebook sent", not u, (u[0][:160] if u else
            f"{ctx['engine'].stats['queries']} queries, {ctx['engine'].stats['replayed']} replayed"))


def _visual(M, title):
    pq = M["per_query"]
    hit = pq[pq["visual"] == title]
    return hit.iloc[0] if len(hit) else None


def _trust_failing(M):
    return [t["name"] for t in M["trust"] if not t["ok"]]


def _truth_checks(M, ns):
    """The injected truth of the clean scenarios (confirmed / reopened)."""
    from synthetic_capture import TRUTH, VISUAL_BY_ID
    out = []
    for key, want in (("slow_visual", "worse"), ("fast_visual", "better")):
        title = VISUAL_BY_ID[TRUTH[key]]["title"]
        r = _visual(M, title)
        out.append((f"report flags the injected {key.replace('_', ' ')} '{title}' as {want} for {M['labels']['B']} "
                    "(CI + round pairs)", r is not None and r["change_cls"] == want, r["change"] if r is not None else "missing"))
    acts = {a["action_seq"]: a for a in M["actions"]}
    for key, want in (("slow_click", "worse"), ("fast_click", "better")):
        a = acts.get(TRUTH[key])
        out.append((f"wait per click: click {TRUTH[key]} ({key.replace('_', ' ')}) is {want}",
                    a is not None and a["cls"] == want,
                    f"{a['a']:.0f} -> {a['b']:.0f} ms ({a['rel']:+.0f}%; {a['why'] or a['cls']})" if a else "missing"))
    NC = M["number_check"]
    m = TRUTH["changed_measure"]
    causes = [c for c, _ in (NC or {}).get("causes", {}).get(m, [])]
    out.append((f"number check flags the changed measure [{m}] as CRITICAL with cause logic_changed",
                bool(NC) and NC["status"] == "differences" and m in NC["flagged"] and "logic_changed" in causes,
                f"status {NC and NC['status']}, flagged {NC and NC['flagged']}, causes {causes}"))
    S = ns["summary_sections"](M)
    nc_blk = [b for b in S["blockers"] if b[0].startswith("Numbers differ")]
    out.append((f"Blockers: the number-check headline names [{m}] and its sub-lines are hidden in HTML (table below)",
                bool(nc_blk) and f"[{m}]" in nc_blk[0][0] and nc_blk[0][2] is True, nc_blk[0][0][:160] if nc_blk else "none"))
    gap = M["refresh_gap_days"]
    names = [t["name"] for t in M["trust"]]
    out.append(("staler refresh of B is in the Data line (not a trust check); data volume (x0.93 < 10%) passes",
                gap == gap and gap > 5 and "older in Branch" in S["fresh"] and "data age" not in names
                and "data volume" in names and "data volume" not in _trust_failing(M), f"gap {gap:.1f} days; " + S["fresh"]))
    out.append(("trust check 'round pairs' replaces 'samples' (2 pairs per level with iterations = 2)",
                "samples" not in names and "round pairs" in _trust_failing(M),
                next((t["text"] for t in M["trust"] if t["name"] == "round pairs"), "")[:160]))
    text_all = " ".join(S["bottom"]) + " " + S["text"] if "text" in S else " ".join(S["bottom"])
    out.append(("the bottom line never says 'looks'; clear facts first", " looks " not in f" {text_all} "
                and S["bottom"][0].startswith("By click"), S["bottom"][0][:160]))
    tv = M["tables"]
    ok = "Sales" in tv.index and tv.loc["Sales", "ratio"] < 1
    out.append(("B has fewer Sales rows (row-count table)", bool(ok),
                f"ratio {tv.loc['Sales', 'ratio']:.3f}" if "Sales" in tv.index else "Sales missing"))
    tr = M["trace"]
    out.append(("server trace matched every query in both models (foreign session ignored)",
                bool(tr) and all(m_ == t_ and t_ > 0 for m_, t_ in tr.values()), str(tr)))
    dr = M["cpu_drivers"] or {}
    inc, dec = (dr.get("inc") or {}).get("name", ""), (dr.get("dec") or {}).get("name", "")
    out.append(("CPU sentence: largest increase = the slow visual, largest decrease = the fast visual (opposite directions)",
                VISUAL_BY_ID[TRUTH["slow_visual"]]["title"] in inc and VISUAL_BY_ID[TRUTH["fast_visual"]]["title"] in dec,
                ns["_cpu_driver_text"](M["cpu_drivers"])[:200]))
    bottom = " ".join(S["bottom"])
    out.append(("bottom line has the mixed-result sentence (slower on click 1, faster on click 3) and says 'all levels pooled'",
                "By click" in bottom and "slower on click 1" in bottom and "faster on click 3" in bottom
                and "all levels pooled" in bottom, bottom[:240]))
    out.append(("bottom line trimmed: no whole-query split, no failure count (they are in trade-offs / KPI row)",
                "whole report queries" not in bottom and "use only measures that match" not in bottom
                and "Failed queries" not in bottom, f"{len(S['bottom'])} sentences"))
    at = M["attrib"]
    out.append(("whole-query differences split: 2 use a measure that differs, 6 use only matching measures",
                bool(at) and len(at["with"]) == 2 and len(at["without"]) == 6, ns["_attrib_text"](M)))
    out.append(("'max' (single run) has no coloured verdict", M["kpis"]["max"]["cls"] in ("few", "same"),
                str(M["kpis"]["max"]["cls"])))
    out.append(("no speed-vs-capacity trade-off built from results that are not both clear",
                not any(t == "Speed vs capacity" for t, _ in S["trade"]) or
                (M["kpis"]["p90"]["cls"] in ("better", "worse") and M["kpis"]["cpu"]["cls"] in ("better", "worse")),
                ", ".join(t for t, _ in S["trade"])))
    return out


def _report_main_checks(ctx, cid):
    """Report files and the main view (confirmed / scheduled_ok)."""
    lake, text = ctx["lake"], ctx["text"]
    html_ = _report_file(lake, cid, "html") or ""
    js = _report_file(lake, cid, "json")
    fp = ctx["ns"].get("APPROVAL_FINGERPRINT")
    n_img = html_.count("src='data:image/png;base64,")
    out = [("report saved as HTML with the wait-per-click chart embedded (main chart + CPU chart)",
            n_img >= 2 and "Executive summary" in html_, f"{len(html_):,} chars, {n_img} embedded images"),
           ("text summary and result JSON saved next to the report",
            bool(_report_file(lake, cid, "txt")) and bool(js), f"txt {'yes' if _report_file(lake, cid, 'txt') else 'no'}, "
                                                                f"json {'yes' if js else 'no'}"),
           ("report header shows the approval fingerprint", bool(fp) and f"approval fingerprint {fp}" in text, str(fp)),
           ("trust line replaces the Warnings box ('N of M checks passed')",
            "checks passed" in text and "Warning:" not in text, re.search(r"Trust: \d+ of \d+ checks passed", text).group(0)
            if re.search(r"Trust: \d+ of \d+ checks passed", text) else "missing"),
           ("Top visuals chart is gone", "Top visuals" not in text, "")]
    if js:
        j = json.loads(js)
        out.append(("result JSON has status, fingerprint, trust, headline classes and number check",
                    j.get("status") == "completed" and j.get("fingerprint") == fp and "trust" in j
                    and "p90" in j.get("headline", {}) and j.get("number_check", {}).get("status") == "differences",
                    json.dumps({k: j.get(k) for k in ("status", "fingerprint", "comparable")})))
    return out


# ---------- checks per scenario ----------------------------------------------------------------------------------
def checks_dry_run(ctx):
    ns, lake, text = ctx["ns"], ctx["lake"], ctx["text"]
    tables = [t for t in TABLES if os.path.isdir(os.path.join(lake, "Tables", t))]
    NC = ns.get("NUMCHECK")
    from synthetic_capture import TRUTH
    m = TRUTH["changed_measure"]
    fp = ns.get("APPROVAL_FINGERPRINT")
    tail = text[text.find("APPROVAL SUMMARY"):] if "APPROVAL SUMMARY" in text else ""
    return [_cells_ok(ctx), _engine_ok(ctx),
            ("dry run writes no tables and no files", not tables and not os.path.isdir(os.path.join(lake, *REPORTS.split("/"))),
             ", ".join(tables) or "nothing written"),
            ("dry run makes no report (REPORT_CID is None)", ns.get("REPORT_CID") is None, str(ns.get("REPORT_CID"))),
            (f"number check runs in the dry run and flags [{m}]", bool(NC) and m in NC["flagged"],
             f"status {NC and NC['status']}, flagged {NC and NC['flagged']}"),
            ("no load in the dry run (no replayed queries)", ctx["engine"].stats["replayed"] == 0,
             f"{ctx['engine'].stats['replayed']} replayed"),
            ("approval summary at the end: rounds, peak vs guard, duration, number-check headline, fingerprint, then "
             "the instruction", all(x in tail for x in ("rounds", "guard", "Duration", "CRITICAL", f'schedule_approval = "{fp}"',
                                                        "confirm_run = True")), tail[:120].replace("\n", " | ")),
            ("Step 3 no longer tells the user to set confirm_run = True before the number check",
             "then set confirm_run = True and run again" not in text, "")]


def checks_confirmed(ctx):
    ns, lake = ctx["ns"], ctx["lake"]
    cid = ns.get("COMPARISON_ID")
    counts = {t: len(_table(lake, t, cid)) for t in TABLES}
    last = _last_status(lake, cid)
    out = [_cells_ok(ctx), _engine_ok(ctx),
           ("all 7 tables have rows for this comparison (incl. lt_run_summary)", all(counts.values()), str(counts)),
           ("final status is 'completed'", last is not None and last["status"] == "completed",
            f"{last['status']}: {last['message']}" if last is not None else "no row")]
    M = ns.get("REPORT_METRICS")
    if M is None:
        return out + [("report metrics computed", False, "REPORT_METRICS is None")]
    ex = _table(lake, "lt_executions", cid)
    out.append(("no failed query executions", bool(len(ex)) and bool(ex["success"].all()),
                f"{int((~ex['success'].astype(bool)).sum())} failed of {len(ex)}"))
    out.append(("interactive run: run mode and fingerprint stored", M["run_mode"] == "interactive" and bool(M["fingerprint"]),
                f"{M['run_mode']} / {M['fingerprint']}"))
    return out + _truth_checks(M, ns) + _report_main_checks(ctx, cid)


def checks_reopen(ctx):
    ns = ctx["ns"]
    M = ns.get("REPORT_METRICS")
    out = [_cells_ok(ctx), _engine_ok(ctx),
           ("reopen writes nothing (Delta logs unchanged, no report files)", _delta_log_files(ctx["lake"]) == ctx["log_before"],
            "compared _delta_log"),
           ("report reopened for the saved comparison", M is not None and M["cid"] == ctx["overrides"]["report_comparison_id"],
            str(M and M["cid"]))]
    if M is None:
        return out
    prev = ctx["prev"]["metrics"]
    now = key_metrics(ns)
    same = all(abs(now["kpis"][k]["a"] - v["a"]) < 1e-6 and abs(now["kpis"][k]["b"] - v["b"]) < 1e-6
               for k, v in prev["kpis"].items() if v["a"] == v["a"] and v["b"] == v["b"] and k in now["kpis"])
    out.append(("reopened report shows the same headline numbers as the run that made it", same,
                f"{len(prev['kpis'])} KPIs compared"))
    hist = ns.get("HISTORY")
    out.append(("History lists the comparison", hist is not None and M["cid"] in set(hist["comparison_id"]),
                f"{0 if hist is None else len(hist)} rows"))
    return out


def checks_reopen_by_id(ctx):
    M = ctx["ns"].get("REPORT_METRICS")
    return checks_reopen(ctx) + (_truth_checks(M, ctx["ns"]) if M is not None else [])


def checks_reopen_legacy(ctx):
    ns = ctx["ns"]
    out = checks_reopen(ctx)
    H = ns.get("HISTORY_ALL")
    row = H[H["comparison_id"] == ctx["overrides"]["report_comparison_id"]] if H is not None and len(H) else None
    out.append(("History computes the old comparison on read (no lt_run_summary row, none written)",
                row is not None and len(row) and row.iloc[0]["source"] == "computed on read"
                and not os.path.isdir(os.path.join(ctx["lake"], "Tables", "lt_run_summary")),
                row.iloc[0]["source"] if row is not None and len(row) else "missing"))
    out.append(("old comparison: no fingerprint -> legacy trend key; report header shows '-'",
                row is not None and len(row) and str(row.iloc[0]["key"]).startswith("legacy-")
                and "approval fingerprint -" in ctx["text"], str(row.iloc[0]["key"]) if row is not None and len(row) else ""))
    return out


def checks_interrupted(ctx):
    ns, lake = ctx["ns"], ctx["lake"]
    cid = ns.get("COMPARISON_ID")
    last = _last_status(lake, cid)
    ex = _table(lake, "lt_executions", cid)
    load = ex[ex["phase"] == "load"] if len(ex) else ex
    lv = [int(x) for x in str(ctx["overrides"]["concurrency_levels"]).split(",")]
    planned = sum(lv) * int(ctx["overrides"]["iterations"]) * 2 * 8
    out = [_cells_ok(ctx), _engine_ok(ctx),
           ("Step 7 was cancelled (KeyboardInterrupt) and tells the user to run Steps 8-10",
            ctx["interrupted"] and "run Steps 8, 9 and 10 to see the partial report" in ctx["text"], ""),
           ("status 'failed' with 'load test interrupted'", last is not None and last["status"] == "failed"
            and "interrupted" in str(last["message"]), f"{last['status']}: {last['message']}" if last is not None else "no row"),
           ("partial load results are saved (0 < saved < planned)", 0 < len(load) < planned, f"{len(load)} of {planned}")]
    M = ns.get("REPORT_METRICS")
    if M is None:
        return out + [("report renders the partial run", False, "REPORT_METRICS is None")]
    S = ns["summary_sections"](M)
    first = S["blockers"][0][0] if S["blockers"] else ""
    out.append(("the first blocker says in plain words that the test was cancelled and what is missing",
                first.startswith("The test did not finish: it was cancelled") and "not measured" in first, first[:200]))
    out.append(("first next step is 'Run the test again'; no precision steps (iterations, more users)",
                bool(S["steps"]) and S["steps"][0].startswith("Run the test again")
                and not any("iterations" in s or "3–5 concurrent" in s for s in S["steps"]), " | ".join(S["steps"])[:200]))
    out.append(("N2: coverage counts rounds that never started (trust check + data-quality note)",
                "coverage" in _trust_failing(M) and any("Coverage" in t for t, _ in ns["data_quality_notes"](M)),
                next((t["text"] for t in M["trust"] if t["name"] == "coverage"), "")[:200]))
    out.append(("N3: context shows the actual rounds per model", "rounds run:" in S["context"], S["context"][:200]))
    out.append(("N4: no coloured verdict from fewer than 5 click-throughs",
                M["kpis"]["journey"]["cls"] not in ("better", "worse") or min(M["kpis"]["journey"]["n_a"], M["kpis"]["journey"]["n_b"]) >= 5,
                f"{M['kpis']['journey']['cls']} (n {M['kpis']['journey']['n_a']}/{M['kpis']['journey']['n_b']})"))
    out.append(("partial run saved as not comparable in lt_run_summary", (lambda s: len(s) == 1 and not bool(s.iloc[0]["comparable"]))(
        _table(lake, "lt_run_summary", cid)), ""))
    return out


def checks_load_issues(ctx):
    ns, lake = ctx["ns"], ctx["lake"]
    cid = ns.get("COMPARISON_ID")
    last = _last_status(lake, cid)
    out = [_cells_ok(ctx), _engine_ok(ctx),
           ("final status is 'completed' (query errors don't stop the test)", last is not None and last["status"] == "completed",
            f"{last['status']}: {last['message']}" if last is not None else "no row")]
    M = ns.get("REPORT_METRICS")
    if M is None:
        return out + [("report metrics computed", False, "REPORT_METRICS is None")]
    ec = M["err_class"]
    out.append(("errors are classified: Branch 1 throttling + 1 timeout, Prod none",
                ec["B"] == {"throttling": 1, "timeout": 1} and not ec["A"], str(ec)))
    fail = _trust_failing(M)
    out.append(("trust line lists throttling (red), the timeout and the round drift",
                {"throttling", "errors", "round drift"} <= set(fail), ", ".join(fail)))
    drift = next((t["text"] for t in M["trust"] if t["name"] == "round drift"), "")
    out.append(("round drift names Branch at 1 user", "Branch" in drift and "1 user" in drift, drift[:200]))
    lv = {x["level"]: x["p90"]["rel"] for x in M["by_level"]}
    S = ns["summary_sections"](M)
    sentence = next((s for s in S["neither"] if s.startswith("By load level")), "")
    out.append(("scaling sentence: Branch's p90 gap is larger at 2 users than at 1 user",
                bool(sentence) and lv.get(2, 0) > lv.get(1, 0) + 20, sentence))
    # sign-off (P1-a): transient failures (1 throttled, 1 timed out) don't remove a visual from the click metrics
    out.append(("SIGN-OFF P1-a: Pros shows click 3 (ApplyFilter) although its only query timed out once in Branch",
                any(p.startswith("Click 3. ApplyFilter") for p in S["pros"]), " | ".join(S["pros"])[:200]))
    out.append(("SIGN-OFF P1-a: #5 and #6 are no longer excluded from click metrics (transient failures only)",
                not ({5, 6} & set(M["failing"])) and M["n_common"] == 8, f"excluded {sorted(M['failing'])}, {M['n_common']} queries in clicks"))
    j = M["kpis"].get("journey", {})
    out.append(("SIGN-OFF P1-a: the click-through is no longer a false Con",
                j.get("cls") != "worse" and not any(c.startswith("Waiting time for one full click-through") for c in S["cons"]),
                f"{j.get('cls')} ({_pct0(j.get('rel'))}; {j.get('why') or _pairs_short(j)})"))
    out.append(("no 'fails only in Branch' blocker for transient failures",
                not any("only in Branch" in h for h, _, _ in S["blockers"]), ""))
    return out


def _pct0(v):
    return "-" if v is None or v != v else f"{v:+.0f}%"


def _pairs_short(k):
    c = k.get("cons") or {}
    return f"{c.get('slower')} slower / {c.get('faster')} faster of {c.get('n')} round pairs" if c else ""


def _skip_checks(ctx, reason_part):
    ns, lake = ctx["ns"], ctx["lake"]
    cid = ns.get("COMPARISON_ID")
    comp = _table(lake, "lt_comparison", cid)
    other = [t for t in TABLES[1:] if os.path.isdir(os.path.join(lake, "Tables", t))]
    ex = ctx["exit"] or {}
    return [_cells_ok(ctx),
            ("scheduled run detected from the Fabric context", "run mode      : scheduled (Fabric context: isForPipeline)"
             in ctx["text"], ""),
            ("skipped: exactly one 'skipped' row with the reason, nothing else written",
             len(comp) == 1 and comp.iloc[0]["status"] == "skipped" and reason_part in str(comp.iloc[0]["message"]) and not other,
             str(comp.iloc[0]["message"])[:200] if len(comp) else "no row"),
            ("the notebook ends with notebook.exit(JSON status 'skipped')", ex.get("status") == "skipped", str(ex)[:160]),
            ("the result JSON is saved next to the reports", bool(_report_file(lake, cid, "json")), ""),
            ("the models were not touched (no connection)", ctx["engine"].stats["connections"] == 0,
             f"{ctx['engine'].stats['connections']} connections")]


def checks_bad_fp(ctx):
    ns, lake = ctx["ns"], ctx["lake"]
    cid = ns.get("COMPARISON_ID")
    comp = _table(lake, "lt_comparison", cid)
    msg = str(comp.iloc[-1]["message"]) if len(comp) else ""
    fp = ns.get("APPROVAL_FINGERPRINT")
    return [_cells_ok(ctx),
            ("scheduled run detected from the Fabric context", "run mode      : scheduled (Fabric context: isForPipeline)"
             in ctx["text"], ""),
            ("an approval mismatch FAILS in Step 3 (raises: the job fails and alerts; no exit JSON)",
             bool(ctx["failed_cell"]) and ctx["failed_cell"].startswith("Step 3") and ctx["exit"] is None, str(ctx["failed_cell"])),
            ("exactly one row, status 'failed', and the message says how to re-approve (dry run, paste, save)",
             len(comp) == 1 and comp.iloc[0]["status"] == "failed" and "changed since the approval" in msg
             and "confirm_run = False" in msg and f'schedule_approval = "{fp}"' in msg and "save the notebook" in msg, msg[:220]),
            ("nothing else written; the result JSON is saved",
             not [t for t in TABLES[1:] if os.path.isdir(os.path.join(lake, "Tables", t))] and bool(_report_file(lake, cid, "json")), ""),
            ("the models were not touched (no connection)", ctx["engine"].stats["connections"] == 0,
             f"{ctx['engine'].stats['connections']} connections")]


def checks_outside(ctx):
    return _skip_checks(ctx, "outside")


def checks_locked(ctx):
    return _skip_checks(ctx, "still running")


def checks_unreachable(ctx):
    ns, lake = ctx["ns"], ctx["lake"]
    cid = ns.get("COMPARISON_ID")
    last = _last_status(lake, cid)
    return [_cells_ok(ctx),
            ("the run FAILS in Step 3 (raises, so the scheduled job fails and alerts)",
             bool(ctx["failed_cell"]) and ctx["failed_cell"].startswith("Step 3") and ctx["exit"] is None, str(ctx["failed_cell"])),
            ("a 'failed' row says the model can't be reached (not 'skipped')",
             last is not None and last["status"] == "failed" and "Cannot connect" in str(last["message"]),
             f"{last['status']}: {str(last['message'])[:160]}" if last is not None else "no row")]


def checks_scheduled_ok(ctx):
    ns, lake = ctx["ns"], ctx["lake"]
    cid = ns.get("COMPARISON_ID")
    last = _last_status(lake, cid)
    M = ns.get("REPORT_METRICS")
    ex = _table(lake, "lt_executions", cid)
    out = [_cells_ok(ctx), _engine_ok(ctx),
           ("final status is 'completed'", last is not None and last["status"] == "completed",
            f"{last['status']}: {last['message']}" if last is not None else "no row"),
           ("all gates passed (approval, window, lock, recent refresh)", "STOP" not in ctx["text"]
            and ctx["text"].count("  ok  ") >= 5, f"{ctx['text'].count('  ok  ')} gates ok"),
           ("warm-up forced on although run_warmup = False", bool(len(ex)) and (ex["phase"] == "warmup").any(),
            f"{int((ex['phase'] == 'warmup').sum()) if len(ex) else 0} warm-up executions")]
    if M is None:
        return out + [("report metrics computed", False, "REPORT_METRICS is None")]
    params = M["params"]
    out.append(("run mode 'scheduled' and the Fabric context are stored in parameters_json",
                M["run_mode"] == "scheduled" and params.get("fabric_context", {}).get("isForPipeline") is True, ""))
    ex_json = ctx["exit"] or {}
    out.append(("the notebook ends with notebook.exit(JSON): completed, fingerprint, headline, report path",
                ex_json.get("status") == "completed" and ex_json.get("fingerprint") == M["fingerprint"]
                and "p90" in ex_json.get("headline", {}) and str(ex_json.get("report_file", "")).endswith(".html"),
                json.dumps({k: ex_json.get(k) for k in ("status", "fingerprint", "report_file")})))
    S = ns["summary_sections"](M)
    out.append(("main view has one trend line; Branch p90 is above the range of the last 5 comparable runs",
                "same approval fingerprint" in S["trend"] and "Branch p90" in S["trend"] and "is above" in S["trend"]
                and "Trend: same approval fingerprint" in ctx["text"], S["trend"][:240]))
    tj = ex_json.get("trend") or {}
    out.append(("exit JSON carries the trend: b_p90 vs its range, position 'above', number of comparable runs",
                tj.get("b_position") == "above" and len(tj.get("b_range") or []) == 2 and tj.get("comparable", 0) >= 7
                and tj.get("b_p90"), json.dumps(tj)[:240]))
    T = ns.get("TREND")
    out.append(("Step 9 trend lists the 5 seeded runs + confirmed + this run", T is not None and len(T["rows"]) >= 7,
                f"{0 if T is None else len(T['rows'])} runs"))
    hist = ns.get("HISTORY")
    out.append(("History collapses the 2 consecutive skipped runs into one row 'skipped ×2' with the reason",
                hist is not None and (hist["status"] == "skipped ×2").sum() == 1
                and "outside" in str(hist.loc[hist["status"] == "skipped ×2", "note"].iloc[0]),
                ", ".join(str(s) for s in (hist["status"] if hist is not None else []))))
    return out + _report_main_checks(ctx, cid)


def notes_interrupted(ctx):
    M = ctx["ns"].get("REPORT_METRICS")
    if M is None:
        return []
    la = M["load_all"]
    return [f"rounds with saved load rows: {sorted(int(r) for r in la['round_no'].unique()) if len(la) else []} "
            f"(the interrupt lands by wall-clock time)"]


NOTES = {"interrupted_load": notes_interrupted}
CHECKS = {"dry_run": checks_dry_run, "confirmed": checks_confirmed, "reopen_by_id": checks_reopen_by_id,
          "reopen_legacy": checks_reopen_legacy, "interrupted_load": checks_interrupted, "load_issues": checks_load_issues,
          "scheduled_bad_fingerprint": checks_bad_fp, "scheduled_outside_window": checks_outside,
          "scheduled_locked": checks_locked, "scheduled_unreachable_model": checks_unreachable,
          "scheduled_ok": checks_scheduled_ok}


# ---------- --all ------------------------------------------------------------------------------------------------
def static_checks():
    sys.path[:0] = [ROOT]
    from build_notebook import parse_py
    out = []
    r = subprocess.run([sys.executable, os.path.join(ROOT, "build_notebook.py"), "--check"], capture_output=True, text=True)
    out.append(("build_notebook.py --check", r.returncode == 0, (r.stdout or r.stderr).strip()[-120:]))
    with open(NOTEBOOK_PY, "rb") as fh:
        cells = parse_py(fh.read().decode("utf-8-sig"))
    bad = []
    for i, (ctype, _, title, src) in enumerate(cells):
        if ctype == "code":
            try:
                compile(src, f"<cell {i}>", "exec")
            except SyntaxError as exc:
                bad.append(f"cell {i} {title}: {exc}")
    out.append(("every code cell compiles", not bad, "; ".join(bad) or f"{sum(c[0] == 'code' for c in cells)} code cells"))
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--all", action="store_true", help="build check + every scenario (each in its own process)")
    ap.add_argument("--scenario", choices=list(SCENARIOS), help="run one scenario in this process")
    ap.add_argument("--out", required=True, help="output folder (keep it outside the repo)")
    ap.add_argument("--legacy", help="folder with lakehouse/ + result.json saved by an older notebook version "
                                     "(default <out>/../fixtures/r1_confirmed)")
    ap.add_argument("--scale", type=float, default=1.0, help="multiply every fake latency (default 1.0)")
    ap.add_argument("--quiet", action="store_true", help="don't echo the notebook output")
    args = ap.parse_args(argv)
    for stream in (sys.stdout, sys.stderr):   # a cp1252 console can't print "−" or "×": never crash on the summary
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    out = os.path.abspath(args.out)
    legacy = os.path.abspath(args.legacy or os.path.join(out, "..", "fixtures", "r1_confirmed"))
    os.makedirs(out, exist_ok=True)
    if args.scenario:
        if SCENARIOS[args.scenario].get("lake_from") == "legacy" and not os.path.isdir(os.path.join(legacy, "lakehouse")):
            print(f"{args.scenario}: NOT RUN (no legacy lakehouse at {legacy})")
            return 0
        res = run_scenario(args.scenario, out, echo=not args.quiet, scale=args.scale, legacy=legacy)
        print(f"\n{args.scenario}: {'PASS' if res['passed'] else 'FAIL'} ({res['seconds']} s)")
        for c in res["checks"]:
            print(f"  {'PASS' if c['ok'] else 'FAIL'}  {c['check']}  [{c['detail']}]")
        for n in res.get("observations", []):
            print(f"  NOTE  {n}")
        return 0 if res["passed"] else 1
    if not args.all:
        ap.error("use --all or --scenario NAME")
    summary, ok_all = [], True
    for c, ok, d in static_checks():
        summary.append(f"  {'PASS' if ok else 'FAIL'}  {c}  [{d}]")
        ok_all &= ok
    for name in ORDER:
        cmd = [sys.executable, os.path.abspath(__file__), "--scenario", name, "--out", out, "--legacy", legacy,
               "--scale", str(args.scale)]
        r = subprocess.run(cmd + (["--quiet"] if args.quiet else []), capture_output=args.quiet, text=True)
        path = os.path.join(out, name, "result.json")
        if SCENARIOS[name].get("lake_from") == "legacy" and not os.path.isdir(os.path.join(legacy, "lakehouse")):
            summary.append(f"{name}: NOT RUN (no legacy lakehouse at {legacy})")
        elif os.path.exists(path) and r.returncode in (0, 1):
            res = _load_result(out, name)
            summary.append(f"{name}: {'PASS' if res['passed'] else 'FAIL'} ({res['seconds']} s)")
            summary += [f"  {'PASS' if c['ok'] else 'FAIL'}  {c['check']}  [{c['detail']}]" for c in res["checks"]]
            summary += [f"  NOTE  {n}" for n in res.get("observations", [])]
            ok_all &= res["passed"]
        else:
            summary.append(f"{name}: FAIL (exit code {r.returncode}) {(r.stderr or '')[-400:]}")
            ok_all = False
    text = "\n".join(["HARNESS SUMMARY"] + summary + [f"OVERALL: {'PASS' if ok_all else 'FAIL'}"])
    with open(os.path.join(out, "summary.txt"), "w", encoding="utf-8") as fh:
        fh.write(text + "\n")
    print("\n" + text)
    return 0 if ok_all else 1


if __name__ == "__main__":
    sys.exit(main())
