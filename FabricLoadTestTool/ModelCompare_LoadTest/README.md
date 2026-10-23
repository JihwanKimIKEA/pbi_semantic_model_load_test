# ModelCompare_LoadTest

An A/B load test for two Fabric / Power BI semantic models, for example **production** against a **feature-branch workspace**. It needs one lakehouse and one Python notebook.

The notebook replays the DAX queries you captured with **Power BI Performance Analyzer** against both models. It generates the load itself as soon as you run it; it does not wait for real users. The result is a side-by-side report of speed, stability, server cost and result differences, plus the trade-offs (definition changes, data volume). A **number check** compares the measures the report uses under a controlled filter, so a difference caused by refresh timing can be told apart from a real one. It doesn't give a merge verdict; the decision is yours.

It builds on [RunLoadTest / RunPerfScenario](../readme.md) (same ADOMD.NET replay technique) and adds:

| | RunLoadTest + RunPerfScenario | ModelCompare_LoadTest |
|---|---|---|
| Models per run | 1 | 2 (A = baseline, B = candidate), alternating rounds A,B / B,A |
| Artifacts | 2 notebooks | 1 notebook (virtual users are Python threads, each with its own connections) |
| Replay | one query at a time, pause after every query | like a report page: the queries of one click run in parallel as captured; think time only between clicks |
| Visual mapping | by event order | by the Performance Analyzer `parentId` chain (exact) |
| Warm-up / cold start | - | warm-up pass excluded from statistics; first-touch time recorded; a model refreshed during the test is warmed up again (refresh guard) |
| Result check | - | row count + order-independent checksum per query, and a **number check**: the report's measures × random business units × a 10-day window both models have loaded, with the likely cause of every difference |
| Server cost | - | traced single-user pass: server duration, CPU, storage-engine queries |
| Model diff | - | measures, calculation items, calculated columns, partitions, expressions, rows per table, capacity |
| Output | CSV + one DataFrame | Delta tables (one row per comparison) + executive summary (blockers, pros, cons, trade-offs) + short report + plain-text summary |

## Files

| File | Use |
|---|---|
| [ModelCompare_LoadTest.py](./ModelCompare_LoadTest.py) | Source with `# CELL n` markers: the guide, then a Markdown heading and a code block for each of the 10 steps. |
| [ModelCompare_LoadTest.ipynb](./ModelCompare_LoadTest.ipynb) | Same cells as an importable Fabric Python notebook, generated from the `.py`. The parameter cell is already tagged, and the code of Steps 2–10 is hidden, so you see only the results. |
| [build_notebook.py](./build_notebook.py) | Regenerates the `.ipynb` from the `.py` (see [Maintainers](#maintainers)). |

## One-time setup

1. **Workspace**: any workspace on a Fabric capacity. A dedicated workspace is recommended so the tooling stays out of production and Git-synced branch workspaces.
2. **Lakehouse**: create `lh_loadtest` (any name; see `lakehouse_name`). Create the folder `Files/LoadTest/Queries/` and upload your `PowerBIPerformanceData.json`. Later query files go to the same folder.
3. **Notebook**: create a notebook named `ModelCompare_LoadTest` and set its language to **Python** (not PySpark).
   - Import the `.ipynb` (recommended), or create 21 cells and paste the blocks from the `.py` in order: a Markdown guide, then for each step a Markdown heading and a code cell. If you paste, hide the code yourself: on each code cell except Parameters, cell `...` menu > *Hide input*.
   - The Parameters code cell (Step 1) must be the parameter cell (cell `...` menu > *Toggle parameter cell*; already set in the `.ipynb`) so pipelines can pass parameters.
   - Attaching the lakehouse is optional: the notebook resolves `host_workspace` + `lakehouse_name` itself.
4. **Permissions**:
   - Read + Build (XMLA read) on both semantic models; XMLA read must be enabled on the capacity. The `INFO.*` functions used for the model diff and the number check may need write permission on the models, depending on the host.
   - The server-cost trace may need Contributor or higher on both workspaces. Without it, the trace is skipped with a warning and everything else still runs.

### Capturing queries (Performance Analyzer)

1. Open the report in the service, then *File > Download this file > A copy of your report with a live connection*.
2. In Power BI Desktop, open the file, start **Performance Analyzer**, interact with the report the way a typical user would, and **Export**.
3. Every user action (Refresh, ChangePage, ChangeFilter, ...) becomes an *action group* (a click). Preflight lists them with numbers, so you can replay a subset with `include_user_actions`. It also shows how many of a click's queries ran at the same time in the capture: the replay uses the same parallelism.

## Run

1. Set the parameters (Step 1) and *Run all* with `confirm_run = False`. This is a **dry run**: it connects, compares the models, runs the **number check** (Step 5: about 15 small single-user queries per model), lists the actions and estimates the duration. Nothing is saved, no load is generated, and no report is shown (Steps 8 and 10 say "dry run only").
2. After approval to load the production model, set `confirm_run = True` and *Run all*.
3. For long runs (large think times or many iterations), run the notebook from a **pipeline or schedule** with the same parameters, so the browser doesn't need to stay open. Results are saved after every round, so an interrupted run can still be reported. See [Scheduled (unattended) runs](#scheduled-unattended-runs).
4. To open a past comparison, set `report_comparison_id` and run Steps 1, 2 and 8 (and 10 for the plain-text summary). Step 9 lists every saved comparison in one table.

### Scheduled (unattended) runs

**You don't need a dry run to get a warm comparison.** A real run (`confirm_run = True`) warms both models itself before measuring anything:

| Step | What runs | Counts in the statistics? |
|---|---|---|
| 3 Preflight | Connect + `EVALUATE {1}` per model (*first touch*; loads a model that was evicted) | Shown as "cold first touch" only |
| 4 Warm-up | Every replayed query once on A, then on B (pages in every column the replay uses, fills the caches) | Shown as "first run" only |
| 6 Server cost | Traced single-user pass, A,B / B,A | Yes (server CPU / duration) |
| 7 Load test | Rounds A,B / B,A | Yes (latency, wait per click) |

A dry run only does the first touch and the small number-check queries, a subset of Step 4. Without a dry run, only the two single measurements "cold first touch" and "first run" change: they then show the real state at the scheduled time, which is what they are meant to show. The dry run is still the right way to check new parameters or a new query file before you schedule them.

**Set it up with a pipeline** (keeps the notebook safe for interactive use):

1. Keep `confirm_run = False` saved in the notebook, so *Run all* in the browser stays a dry run. The notebook's own *Schedule* runs with the saved values; to pass `confirm_run = True` only to the scheduled run, use a pipeline.
2. Create a **Data pipeline**, add a **Notebook** activity and select `ModelCompare_LoadTest`.
3. *Settings* > *Base parameters*: add `confirm_run`, type **Bool**, value `true`. Add any other parameter you want to override for the scheduled run (same names as in Step 1).
4. *General* > *Timeout*: longer than the TOTAL estimate that preflight prints, plus margin (queries are slower under load, and a re-warm adds a single-user pass).
5. *Home* > *Schedule*: pick a start time when neither model is refreshed for the estimated duration. Check both models' refresh schedules, and for Direct Lake models whether *Keep your Direct Lake data up to date* (automatic updates) reframes the model while ETL jobs write the tables at night.
6. Afterwards, open the result with Step 9 (History) and `report_comparison_id` + Steps 1, 2, 8 and 10.

On/off parameters accept `True`/`False`, `0`/`1` or the text `true`/`false`/`yes`/`no` (any case). Any other value stops the run in preflight, so a value like `"false"` passed as text can never start a real test.

**Refresh guard** (`rewarm_after_refresh = True`, needs `run_warmup = True`): a refresh or Direct Lake reframe clears a model's caches, which would make the comparison unfair. The notebook reads each model's last refresh time after the warm-up, before and after the server-cost pass, and before and after every load round:

- Refreshed since its last warm-up → the model is warmed up again (phase `rewarm`, not in the statistics) before the next measurement.
- Refreshed while a load round of that model ran → that round is stored as `load_discarded` (not in the statistics), the model is warmed up again, and the round is repeated with the same round number. After 2 repeats the last attempt is kept, and the report flags the refresh.
- Refreshed during the server-cost pass → the model is warmed up again; the report warns that its server cost may include cold queries (the pass isn't repeated).

Each event is stored in `lt_model_profile` (`item_type = refresh_guard`) and listed in Step 10. A refresh the guard handled is a data-quality note in the report, not a warning.

### Finding the results

The notebook has 10 steps. Each step is a heading followed by one code cell. In the imported `.ipynb`, the code of Steps 2–10 is hidden, so under each heading you see only that step's result.

- **Contents** on the notebook command bar lists the steps; select one to jump to its result.
- To read or change the code of a step: cell `...` menu > *Show input*. *Hide input* hides it again.
- The Parameters cell (Step 1) stays visible, because it's the cell you edit.

| Step | Shows |
|---|---|
| 1 Parameters | Your inputs (the only cell you edit) |
| 2 Setup | One "Setup OK" line |
| 3 Preflight | Plan, clicks with their parallelism, model differences, peak concurrent queries, estimated duration |
| 4 Warm-up and result comparison | First-run times, which whole-query results differ, rows per table |
| 5 Number check | Detected columns, window, business units, then either "all measures match" or **CRITICAL** with the measures that differ, where and why |
| 6 Server cost | Server time and CPU per model |
| 7 Load test | Progress per round (query latency and wait per click), then the totals |
| 8 Report | Executive summary, wait-per-click chart, collapsed *Details for analysts* |
| 9 History | One row per saved comparison |
| 10 Text summary | The executive summary and the same numbers as plain text, for sharing |

### Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `test_name` | | Prefix of the `comparison_id` |
| `a_label`, `a_workspace`, `a_model` | Prod | Baseline model |
| `b_label`, `b_workspace`, `b_model` | Branch | Candidate model |
| `host_workspace`, `lakehouse_name` | `""`, `lh_loadtest` | Where the query file and results live (`""` = notebook workspace) |
| `query_file` | `Files/LoadTest/Queries/PowerBIPerformanceData.json` | Path inside the lakehouse |
| `include_user_actions` | `""` | `""` = all queries in captured order; or labels (`"Refresh,ChangePage"`) or action numbers (`"2,4"`) |
| `concurrency_levels` | `"1,3,5"` | Virtual users per step |
| `iterations` | `3` | Rounds per model per level (one round = one full click-through per virtual user) |
| `think_time_between_actions_sec` | `60` | Pause between two clicks (the queries of one click run in parallel, without pauses) |
| `cooldown_between_rounds_sec` | `0` | Pause between rounds |
| `ramp_up_sec` | `0` | Delay between starting each virtual user in a round |
| `query_timeout_sec` | `600` | Per query |
| `run_warmup`, `compare_results`, `checksum_float_decimals` | `True`, `True`, `6` | Warm-up pass and whole-query result checksums |
| `run_number_check` | `True` | Step 5 (also runs in the dry run) |
| `number_check_bu_count` | `3` | Random business units in the controlled filter |
| `number_check_days` | `10` | Window length; it ends one day before the latest date both models have loaded |
| `number_check_tolerance_pct` | `0.0001` | Relative difference (%) still counted as equal; text values must match exactly |
| `number_check_seed` | `0` | `0` = a new random pick (the seed is printed and saved); a saved seed repeats a pick |
| `number_check_bu_column`, `number_check_date_column` | `""` | `""` = auto-detect; or e.g. `"'Business Unit'[Business Unit]"`, `"'Date'[Date]"` |
| `run_server_trace` | `True` | Traced single-user pass per model |
| `run_load_test` | `True` | Concurrent load phase |
| `rewarm_after_refresh` | `True` | Refresh guard: re-warm a model refreshed during the test and repeat the load round it overlapped (see [Scheduled (unattended) runs](#scheduled-unattended-runs)). Needs `run_warmup = True` |
| `confirm_run` | `False` | Safety switch: `False` = dry run (checks + number check, nothing saved, no report); `True` = run and save a new test. From a pipeline, pass it as type Bool |
| `max_concurrency_guard` | `5` | Refuses concurrency levels (virtual users) above this value |
| `max_concurrent_queries_guard` | `15` | Refuses a run whose peak concurrent queries per model (virtual users × the largest parallelism of a click) exceeds this value |
| `noise_band_pct` | `5` | Changes within ±5% are shown as "no meaningful change" |
| `top_n_queries` | `15` | Visuals shown in the "Top visuals" chart |
| `report_comparison_id` | `""` | `""` = the test this run makes (only with `confirm_run = True`); otherwise a saved `comparison_id` to reopen |
| `app_name` | `ModelCompare_LoadTest` | Sent as *Application Name*; visible in traces and workspace monitoring |

The think time between clicks dominates the duration: one click-through takes about Σ captured click times + (clicks − 1) × `think_time_between_actions_sec`. Preflight prints the computed total before anything runs.

## How it works

- **Step 3 - Preflight**:
   - Parses the Performance Analyzer file and builds the click plan: for each click, the largest number of its queries that ran at the same time in the capture (its *parallelism*) and its captured wall time.
   - Connects to both models; the first connection time is the *first-touch* latency and shows whether a model was cold or evicted.
   - Compares definitions via `INFO.*` DAX functions, checks the capacity, checks `max_concurrent_queries_guard` and estimates the duration.
- **Step 4 - Warm-up**:
   - Runs every query once per model, single user. Records the first-run latency, row count and an order-independent result checksum of the whole query.
   - Then counts rows per table. This runs after the warm-up so it doesn't distort the cold measurement.
   - Whole-query results often differ only because the Branch snapshot hasn't loaded the latest days; Step 5 settles that.
- **Step 5 - Number check** (controlled filter; also in the dry run; it never stops the run):
   - *Measures*: the model measures the replayed queries use (report-level `DEFINE MEASURE` measures are skipped). Measures that exist in one model only are listed, not compared.
   - *Columns*: the date column is the key of a table marked as date table (else a Date/Calendar table's date column, preferring columns with relationships and columns the report uses). The business-unit column is a text column named like *Business Unit* / *BU* (Name preferred over Code, visible, used by the report) with 2–1,000 values. Both must exist in both models; the chosen columns and the other candidates are printed. Override them with `number_check_bu_column` / `number_check_date_column`.
   - *Window*: for each model, the latest date with rows of each fact table that the measures use, capped at that table's last refresh (so plan / forecast rows in the future are ignored). A model's latest date is the earliest of its current fact tables, so every table has loaded it. The window is `number_check_days` days ending **one day before** the earlier of the two models' dates (usually the Branch snapshot). The same literal dates go to both models.
   - *Business units*: `number_check_bu_count` random business units with rows in the window in both models (the seed is saved). If the column doesn't filter the fact tables, the check is skipped.
   - *Comparison*: each measure per business unit and for the selected business units together (the measure's own total), in batches of 20; a failing batch is split until the failing measure is found. Numbers are equal within `number_check_tolerance_pct`; text must match exactly.
   - *Where and why*: for each measure that differs, the values per date and business unit, the rows of its tables in the window, and the likely cause as facts: its definition differs, an object it depends on differs (via `INFO.CALCDEPENDENCY`, or by reading the expressions when that isn't available), rows in the window differ (and changed data-source settings), it uses `TODAY()`/`NOW()`, it fails in one model only, or otherwise *unexplained — investigate*.
   - Any difference (or an error in one model only) is **CRITICAL** and becomes a blocker in the report.
- **Step 6 - Server cost**:
   - Traces both models **at the same time** during one single-user pass, with queries alternating A,B / B,A. Both models are therefore measured at the same moment and at the same distance from their warm-up, so neither gets a cache advantage.
   - Only this notebook's sessions are matched, so other users' queries are ignored and never stored.
   - Only the notebook's own traces are dropped afterwards.
   - With the refresh guard, a model refreshed since its warm-up is warmed up again first; a refresh during the pass is reported.
- **Step 7 - Load test**:
   - For each level and iteration, rounds alternate A,B → B,A → A,B. This cancels time-of-day and capacity drift.
   - With the refresh guard, a model refreshed before its round is warmed up again, and a round that overlapped a refresh of its model is discarded and repeated (at most 2 repeats).
   - Each virtual user clicks through the report: the queries of one click run in parallel up to the click's captured parallelism, each parallel slot on its own connection; then it waits `think_time_between_actions_sec` before the next click.
   - Peak concurrent queries per model = virtual users × the largest parallelism; preflight prints it and refuses runs above `max_concurrent_queries_guard`.
   - Connections are (re)opened between clicks, with a fresh token before tokens expire, so connecting never counts as waiting time.
   - Cancelling the cell stops all virtual users.

## Reading the report

The report has a short main view for decision makers (the executive summary and one chart) and one collapsed **Details for analysts** section with everything else.

**Main view**

- **Executive summary**. It is built only from the stored results, so it adapts to any pair of models, and it states facts only (no merge decision):
  - **Context**: test time, capacity, load, replay mode, and the data freshness of both models ("Prod refreshed …; Branch refreshed … (n days older)").
  - **Blockers** (red, only when something applies): numbers that differ under the controlled filter (up to 5 measures with values, where and cause), queries that fail only in B, an incomplete run.
  - **Warnings** (amber): different capacities, a model refreshed during the test that the refresh guard couldn't handle, a refresh during the server-cost pass.
  - **Bottom line**: speed for users, capacity use (server CPU), the number check, and failures; then the plain-language KPIs: slowest 10% of visuals, slowest single visual, typical visual, one full click-through, server CPU (drives CU usage) and failed queries. Changes read like "about 2.1× faster (−52%)"; changes smaller than 100 ms are marked "not noticeable to users".
  - **Pros** (B better) and **Cons** (B worse): only changes that are statistically clear at 95% confidence, with A → B and the 95% range; clearly faster / slower clicks; fewer / more failures. Then which changes are **not statistically clear** (the change is larger than the noise band, but its 95% range includes no change) or **no meaningful change** (within the noise band), and where the server CPU difference comes from.
  - **Trade-offs and risks**: speed vs capacity pulling in opposite directions, changed logic (measures, calculation items, calculated columns) with its number-check outcome, whole-query result differences in the light of the number check, different data volume, different data source or parameters.
  - **Questions to resolve**, **next steps** (including how to confirm the exact CU in the Capacity Metrics app), and a **Copy as text** block for email or Teams.
- When the number check found differences, a compact table of those measures follows the summary.
- **Wait per click** chart: for each click (user action: page change, filter, slicer, ...), the typical time a user waits under load, A next to B: from the first query start to the last query end of the click. It shows *where* in the report the difference is.

**Details for analysts** (collapsed; click to open, no re-run needed)

- **Number check**: the settings (columns and how they were found, window and how it was chosen, business units and seed, tolerance), the latest date per table and model, the measures that differ (values, change, where, cause), the values per date and business unit, the rows in the window, and the measures that match.
- **At a glance** scorecard: B compared with A for latency (median, p90, p95, max), click-path wait, error rate, cold first touch, first run, and server CPU/duration.
  - Each metric shows Δ, Δ% and a **95% bootstrap confidence interval**.
  - **Green/red only shows the direction** of the change. Grey means the change is inside the noise band (no meaningful change). Amber means the change is larger than the noise band, but its 95% confidence interval crosses 0 (not statistically clear: a repeat test could show the opposite).
- **By concurrency level**: medians and p90 per level, with confidence intervals and errors.
- **Top visuals** chart: the visuals whose wait changed most.
- **Server CPU per query** scatter chart: each dot is one query, measured once per model with a single user. On the dashed line both models use the same CPU; above it B uses more (costs more capacity), below it B uses less. Both axes use a log scale, so small and large queries fit in one chart. The 5 largest changes are numbered and listed under the chart.
- **Each visual: how long a user waits**: one row per visual, biggest change first, in plain words:
  - *Typical wait* in A and in B: the middle value of all the runs of that visual's query in the load test (half of the runs were faster, half slower).
  - *Change*: for example "about 5.1× slower (+413%)", "about the same (−2%)" (within the noise band), or "no clear difference (−21%)" (the gap is small compared with how much the runs varied, so it may be chance). Failed runs are added, e.g. "failed 3× in Branch".
  - *Same numbers?*: whether both models returned the same result for the whole query (Step 4).
- **Errors** (if any).
- **Data-quality notes**: few samples, incomplete trace, cold start, capacity, last refresh time per model (and a note if a model was refreshed during the test: handled by the refresh guard, or a warning when it wasn't), interrupted run.

Comparisons saved before the page-load replay existed are shown with their original meaning: the click wait is the sum of the click's query times (sequential replay), and the number check is "not available".

**History** (Step 9): one row per saved comparison, with its query file, number of queries, replay mode, settings, the number-check outcome (`match (N)`, `n differ`, `skipped`, `error`, `-` = saved before the number check existed) and headline numbers. Rows can test different query files and replay modes, so compare rows only when they used the same query file, replay and settings. There are no trend charts.

**Text summary** (Step 10): the executive summary as plain text, then the number check, the model definition differences, the tables whose row count differs, and the top 15 queries by latency and by CPU change. Copy the whole output to share the exact numbers. Run it after Step 8. After a dry run (`confirm_run = False`) without `report_comparison_id`, it prints a short message instead.

Click-based metrics only use queries that succeeded in both models; the number of excluded failing queries is shown.

## Lakehouse tables

| Table | Grain |
|---|---|
| `lt_comparison` | One row per status change of a run (latest row wins): parameters, models, query file hash, status |
| `lt_query_catalog` | Run × query: action, visual, captured timing, query text |
| `lt_model_profile` | Run × model × item: first touch, object counts, storage modes, definition hashes, rows per table, capacity, last refresh (at start and end), refresh-guard events (`refresh_guard`: `item_name` = when, e.g. `before round 4` / `during round 4: repeated`, or `check failed` when a refresh time couldn't be read, after which refreshes of that model count as not handled; `value_text` = the refresh time; `value_num` = round), and the number check (below) |
| `lt_executions` | Every query execution (phase `warmup` / `trace` / `load`, plus `rewarm` / `load_discarded` from the refresh guard), timing, rows, checksum, error. The statistics use `load` (and `warmup` for the first run) only |
| `lt_server_metrics` | Run × model × query: server duration, CPU, storage-engine queries / CPU / cache matches |
| `lt_result_compare` | Run × query: rows and checksum A vs B |

All tables are keyed by `comparison_id`, so you can also build your own Power BI report on them.

`lt_comparison.parameters_json` also holds `replay_mode` (`page_load`; comparisons without it are sequential replays) and `click_parallelism` (`{action number: parallel queries}`).

The number check is stored in `lt_model_profile` with these `item_type` values (rows shared by both models have `model_key = "AB"`; keys with several parts are JSON lists in `item_name`):

| item_type | model_key | item_name | value |
|---|---|---|---|
| `numcheck_config` | AB | `status` (`match` / `differences` / `skipped` / `error`; `value_num` = measures that differ), `reason`, `date_column`, `bu_column` (+ `_source`), `anchor_date`, `anchor_rule`, `window_start`, `window_end`, `bus`, `bu_candidates`, `seed`, `tolerance_pct`, `measures_compared`, `measures_unchecked`, `only_in_A`, `only_in_B`, `query_scoped_skipped`, `dependency_source`, `fact_tables` | `value_text` / `value_num` |
| `numcheck_data_date` | A / B | fact table | `value_text` = JSON {latest, max, cap, refreshed_utc, mode, use}; `value_num` 1 = sets the window |
| `numcheck_value` | A / B | `[measure, business unit]` (`null` = the selected business units together) | `value_num` (number) or `value_text` (text) |
| `numcheck_value_by_date` | A / B | `[measure, business unit, date]` (only for measures that differ) | as above |
| `numcheck_rows` | A / B | `[table, business unit]` | `value_num` = rows in the window |
| `numcheck_error` | A / B | measure | `value_text` = error |
| `numcheck_result` | AB | measure | `value_text` = `same` / `different` / `error in A` / `error in B` / `error in both`; `value_num` = largest \|change %\| |
| `numcheck_cause` | AB | `[measure, cause code]` | `value_text` = the fact sentence |

## Notes and limitations

- **Production impact**: the load runs on the capacity that real users share. Start with low levels; `confirm_run`, `max_concurrency_guard` and `max_concurrent_queries_guard` exist for this reason. Parallel replay sends up to *virtual users × parallelism* queries at the same moment. The dry run also sends the number-check queries (single user, small) to both models.
- **Fairness**: both workspaces should be on the same capacity; preflight checks this. If B uses a different data source or volume, the speed and cost deltas combine the definition changes and the data difference. The report shows both but doesn't attribute the cause.
- **Number check**: it removes refresh-timing noise only inside the window. Data corrected in place in Prod after the Branch was refreshed, measures that use `TODAY()`/`NOW()`, calculation groups with a default selection, and measures that exist in one model only can still differ, and are reported with that fact. With a different business-unit or date column, or when detection fails, set the override parameters.
- **Cold vs warm**: no cache clearing or forced cold state is used, to avoid hurting production users. Cold behaviour is visible through first touch and the warm-up first run.
- **Refreshes and caches**: a refresh or Direct Lake reframe clears a model's caches. The notebook records each model's last refresh time at the start and the end, and the refresh guard (`rewarm_after_refresh`) re-warms a refreshed model and repeats the load round it overlapped. A refresh is detected when it commits: an Import refresh that is still processing uses capacity CPU but isn't detected until then, so schedule tests outside refresh windows. Repeated identical queries can be served from the engine's cache, so a DAX change that makes queries cache-friendly can look dramatically faster on repeats. Compare the warm-up (first run) numbers with the load-test numbers to see both effects.
- **Trace**: `sempy` traces are best-effort. If events are incomplete, the run is flagged in the data-quality notes rather than failed.
- **Checksums**: rows are compared order-independently after rounding floats (`checksum_float_decimals`). A difference is information, not an error.
- **Client sizing**: the default Python notebook (2 vCores) handles the default levels. Preflight suggests a `%%configure` cell (`{"vCores": 8}`) at the top when the peak concurrent queries exceed 20.
- **Timings**: preflight estimates use the Desktop timings captured by Performance Analyzer; under load, queries are usually slower. Latencies of page-load and sequential (older) comparisons are not comparable.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Cannot connect to ...` | Check Read + Build permission and XMLA read on the capacity. Workspace names with special characters are URL-encoded; if that fails, the notebook retries with `<name> - <workspace guid>` automatically. |
| `Could not read 'Files/...'` | Check `lakehouse_name`, `host_workspace` and the folder / file name. |
| `Server trace not available` | Missing permission for tracing on that workspace. The rest of the test still runs. |
| `concurrency ... exceeds max_concurrency_guard` | Raise `max_concurrency_guard` only after approval. |
| `Peak concurrent queries ... above max_concurrent_queries_guard` | Lower `concurrency_levels`, replay fewer clicks, or raise the guard only after approval. |
| `Number check skipped: no business-unit column ...` / `no date column ...` / `... doesn't filter ...` | Set `number_check_bu_column` / `number_check_date_column` and run the dry run again. |
| Number check: dependencies from "measure expressions" | `INFO.CALCDEPENDENCY` isn't available (or not permitted) on that model; the expressions are read instead. |
| Notebook errors on `sempy` / `notebookutils` | The notebook must be a Fabric **Python** notebook. |
| After import, the code of Steps 2–10 is visible | The hidden setting wasn't kept. On each of those code cells: cell `...` menu > *Hide input*, then save the notebook. |

## Maintainers

The `.py` is the source of truth. After editing it, regenerate the notebook and check that both are in sync:

```
python build_notebook.py          # writes ModelCompare_LoadTest.ipynb (keeps the cell ids)
python build_notebook.py --check  # exit code 1 if the .ipynb is out of sync
```
