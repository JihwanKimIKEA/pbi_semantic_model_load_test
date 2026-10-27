# ModelCompare_LoadTest_ScheduleRun
An A/B load test for two Fabric / Power BI semantic models, for example **production** against a **feature-branch workspace**. It needs one lakehouse and one Python notebook.

The notebook replays the DAX queries you captured with **Power BI Performance Analyzer** against both models. It generates the load itself as soon as you run it; it does not wait for real users. The result is a side-by-side report of speed, stability, server cost and result differences, plus the trade-offs (definition changes, data volume). A **number check** compares the measures the report uses under a controlled filter, so a difference caused by refresh timing can be told apart from a real one. It doesn't give a merge verdict; the decision is yours.

It builds on [RunLoadTest / RunPerfScenario](../readme.md) (same ADOMD.NET replay technique) and adds:

| | RunLoadTest + RunPerfScenario | ModelCompare_LoadTest_ScheduleRun |
|---|---|---|
| Models per run | 1 | 2 (A = baseline, B = candidate), alternating rounds A,B / B,A |
| Artifacts | 2 notebooks | 1 notebook (virtual users are Python threads, each with its own connections) |
| Replay | one query at a time, pause after every query | like a report page: the queries of one click run in parallel as captured; think time only between clicks |
| Visual mapping | by event order | by the Performance Analyzer `parentId` chain (exact) |
| Warm-up / cold start | - | warm-up pass excluded from statistics; first-touch time recorded |
| Result check | - | row count + order-independent checksum per query, and a **number check**: the report's measures × random business units × a 10-day window that ends a week before the latest date both models have loaded, with the likely cause of every difference |
| Server cost | - | traced single-user pass: server duration, CPU, storage-engine queries |
| Model diff | - | measures, calculation items, calculated columns, partitions, expressions, rows per table, capacity |
| Output | CSV + one DataFrame | Delta tables (one row per comparison) + executive summary (blockers, pros, cons, trade-offs) + short report + plain-text summary |

## Files

| File | Use |
|---|---|
| [ModelCompare_LoadTest_ScheduleRun.py](./ModelCompare_LoadTest_ScheduleRun.py) | Source with `# CELL n` markers: the guide, then a Markdown heading and a code block for each of the 10 steps. |
| [ModelCompare_LoadTest_ScheduleRun.ipynb](./ModelCompare_LoadTest_ScheduleRun.ipynb) | Same cells as an importable Fabric Python notebook, generated from the `.py`. The parameter cell is already tagged, and the code of Steps 2–10 is hidden, so you see only the results. |
| [build_notebook.py](./build_notebook.py) | Regenerates the `.ipynb` from the `.py` (see [Maintainers](#maintainers)). |
| [tests/](./tests/) | Offline test harness (fake Fabric, fake models, synthetic capture); not needed in Fabric (see [Maintainers](#maintainers)). |

## One-time setup

1. **Workspace**: any workspace on a Fabric capacity. A dedicated workspace is recommended so the tooling stays out of production and Git-synced branch workspaces.
2. **Lakehouse**: create `lh_loadtest` (any name; see `lakehouse_name`). Create the folder `Files/LoadTest/Queries/` and upload your `PowerBIPerformanceData.json`. Later query files go to the same folder.
3. **Notebook**: create a notebook named `ModelCompare_LoadTest_ScheduleRun` and set its language to **Python** (not PySpark).
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

1. Set the parameters (Step 1) and *Run all* with `confirm_run = False`. This is a **dry run**: it connects, compares the models, runs the **number check** (Step 5: about 15 small single-user queries per model), lists the actions and estimates the duration. Nothing is saved and no load is generated. It ends with an **approval summary** (Step 8): rounds, peak concurrent queries vs the guard, estimated duration, the number-check headline and the **approval fingerprint**, then what to do next.
2. After approval to load the production model, set `confirm_run = True` and *Run all*.
3. To run it unattended (nightly, from a pipeline), see [Scheduled runs](#scheduled-runs). Results are saved after every round, so an interrupted run can still be reported: Step 7 tells you to run Steps 8–10 for the partial report.
4. To open a past comparison, set `report_comparison_id` and run Steps 1, 2 and 8 (and 10 for the plain-text summary). Step 9 lists every saved comparison in one table, then the trend of the runs with the same fingerprint.

## Scheduled runs

The notebook is schedule-ready; the schedule itself is created in the Fabric UI (notebook > *Run* > *Schedule*, or a pipeline with a Notebook activity). A schedule or pipeline passes no parameters, so the defaults in Step 1 apply: set them, including `schedule_approval`, and save the notebook.

**Run mode.** `run_mode = "auto"` detects a pipeline or scheduled job from `notebookutils.runtime.context` (e.g. `isForPipeline`); `"interactive"` / `"scheduled"` force it (a pipeline can pass `"scheduled"`). Every run stores the full context in `parameters_json.fabric_context`, so the first real scheduled run shows which keys Fabric sets. In scheduled mode `confirm_run` is ignored: the gates decide.

**Is a dry run needed for scheduled runs?** Not as a separate scheduled run. Every run does the preflight and the number check before any load. What is needed is a **one-time interactive dry run that approves the exact load settings**: it prints the approval fingerprint (12 characters: both models, the query file's sha256, `include_user_actions`, the load shape, timeouts, the `run_*` switches, `warmup_passes`, the number-check scope and the guards; report-only settings and the seed are left out). Paste it into `schedule_approval`. Any change to those settings or the query file changes the fingerprint, and scheduled runs **fail** until a new dry run is approved (a mismatch never heals by itself, so it must alert someone).

**Is a warm-up needed?** Yes; scheduled runs always warm up (`run_warmup` is forced on). Off-hours, models (especially branch workspaces and Direct Lake / large models) are often evicted or paged out, and the cold start would leak into the load statistics. The cold state stays visible as first touch and first-run pass. `warmup_passes` > 1 adds unmeasured extra passes when caches need longer to settle.

**Gates** (checked before anything is written; the approval, window and lock gates before the models are touched):

| Gate | When it does not pass | Result |
|---|---|---|
| approval | `schedule_approval` is empty or doesn't match the fingerprint of the current settings + query file | **fail**: the message says how to re-approve (interactive dry run, paste `schedule_approval = "…"` into Step 1, save) |
| time window | the run starts outside `schedule_window` (e.g. `"22:00-05:00"`, may cross midnight) in `schedule_timezone` | skip |
| no overlapping run | another run of the same `test_name` is `running` and updated recently (a row older than 2× the estimated duration, at least 30 min, counts as crashed) | skip |
| no recent refresh | a model refreshed or reframed less than `schedule_min_minutes_since_refresh` minutes ago (empty caches) | skip |

**Skip vs fail.** A skip (window, lock, recent refresh: these heal by themselves) writes one `lt_comparison` row with status `skipped` and the reason, saves `Files/LoadTest/Reports/<comparison_id>.json` and ends the notebook with `notebookutils.notebook.exit(json)`: the job succeeds, and History shows why (consecutive skips are one row). A failure writes a `failed` row with the reason and raises, so the job fails and alerts work: an approval mismatch (someone must re-approve), and a model that can't be reached (e.g. the branch workspace was deleted after the merge: disable the schedule; otherwise check the schedule owner's permissions). Different capacities are not a gate: the run is marked *not comparable*.

**Output.** Nobody watches a scheduled run, so the report is also saved: `Files/LoadTest/Reports/<comparison_id>.html` (exactly what Step 8 shows, charts embedded), `.txt` (Step 10) and `.json` (the result). The notebook ends with the same JSON as its exit value (status, comparison_id, fingerprint, comparable, trust checks, blockers, headline changes with 95% range and class, number check, errors by class, file paths, and the **trend**: `b_p90` / `a_p90` with the range of the last comparable runs and `above` / `inside` / `below`, or `null` with fewer than 3 comparable runs), so a pipeline can branch on it and send a Teams message or email, e.g. when `trend.b_position` is `above`. The main view of a scheduled run has one trend line (see [History](#reading-the-report)). The token comes from the schedule owner: that account needs the permissions in [One-time setup](#one-time-setup).

### Finding the results

The notebook has 10 steps. Each step is a heading followed by one code cell. In the imported `.ipynb`, the code of Steps 2–10 is hidden, so under each heading you see only that step's result.

- **Contents** on the notebook command bar lists the steps; select one to jump to its result.
- To read or change the code of a step: cell `...` menu > *Show input*. *Hide input* hides it again.
- The Parameters cell (Step 1) stays visible, because it's the cell you edit.

| Step | Shows |
|---|---|
| 1 Parameters | Your inputs (the only cell you edit) |
| 2 Setup | One "Setup OK" line |
| 3 Preflight | Run mode, approval fingerprint, scheduled-run gates, plan, clicks with their parallelism, model differences, peak concurrent queries, estimated duration |
| 4 Warm-up and result comparison | First-run times, which whole-query results differ, rows per table |
| 5 Number check | Detected columns, window, business units, then either "all measures match" or **CRITICAL** with the measures that differ, where and why |
| 6 Server cost | Server time and CPU per model |
| 7 Load test | Progress per round (query latency and wait per click), then the totals |
| 8 Report | Executive summary, wait-per-click chart, collapsed *Details for analysts*; after a dry run, the approval summary |
| 9 History | One row per saved comparison, then the trend of the runs with the same fingerprint |
| 10 Text summary | The executive summary and the same numbers as plain text, for sharing (saved, with the result JSON) |

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
| `number_check_days` | `10` | Window length (days) |
| `number_check_end_offset_days` | `7` | The window ends this many days before the latest date both models have loaded (usually the Branch snapshot), so data that arrives late or is corrected after the fact has settled in both. `1` = the day before (the old behaviour). Not part of the approval fingerprint |
| `number_check_tolerance_pct` | `0.0001` | Relative difference (%) still counted as equal; text values must match exactly |
| `number_check_seed` | `0` | `0` = a new random pick (the seed is printed and saved); a saved seed repeats a pick |
| `number_check_bu_column`, `number_check_date_column` | `""` | `""` = auto-detect; or e.g. `"'Business Unit'[Business Unit]"`, `"'Date'[Date]"` |
| `run_server_trace` | `True` | Traced single-user pass per model |
| `run_load_test` | `True` | Concurrent load phase |
| `confirm_run` | `False` | Safety switch: `False` = dry run (checks + number check + approval summary, nothing saved, no report); `True` = run and save a new test. Ignored by scheduled runs (the gates decide) |
| `max_concurrency_guard` | `5` | Refuses concurrency levels (virtual users) above this value |
| `max_concurrent_queries_guard` | `15` | Refuses a run whose peak concurrent queries per model (virtual users × the largest parallelism of a click) exceeds this value |
| `run_mode` | `"auto"` | `auto` = detect a pipeline / scheduled job from the Fabric context; or `interactive` / `scheduled` |
| `schedule_approval` | `""` | Approval fingerprint printed by an interactive dry run; a scheduled run needs a match |
| `schedule_window` | `""` | Allowed start time of a scheduled run, e.g. `"22:00-05:00"`; `""` = any time |
| `schedule_timezone` | `"UTC"` | Time zone of `schedule_window` (e.g. `"Europe/Berlin"`) |
| `schedule_min_minutes_since_refresh` | `30` | A scheduled run is skipped when a model refreshed more recently |
| `warmup_passes` | `1` | Warm-up passes per model: the first is measured (first run + checksum), further passes are unmeasured |
| `save_report_files` | `True` | Save the report (`.html`), text summary (`.txt`) and result (`.json`) to `Files/LoadTest/Reports/` |
| `noise_band_pct` | `5` | Changes within ±5% are shown as "no meaningful change" |
| `top_n_queries` | `15` | Queries per table in the Step 10 text summary |
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
   - *Window*: for each model, the latest date with rows of each fact table that the measures use, capped at that table's last refresh (so plan / forecast rows in the future are ignored). A model's latest date is the earliest of its current fact tables, so every table has loaded it. The window is `number_check_days` days ending **`number_check_end_offset_days` days before** (default 7) the earlier of the two models' dates (usually the Branch snapshot): the newest days of a snapshot are often incomplete (late-arriving rows, corrections loaded after the fact), and Prod keeps refreshing them after the Branch snapshot was taken. The same literal dates go to both models.
   - *Business units*: `number_check_bu_count` random business units with rows in the window in both models (the seed is saved). If the column doesn't filter the fact tables, the check is skipped.
   - *Comparison*: each measure per business unit and for the selected business units together (the measure's own total), in batches of 20; a failing batch is split until the failing measure is found. Numbers are equal within `number_check_tolerance_pct`; text must match exactly.
   - *Where and why*: for each measure that differs, the values per date and business unit, the rows of its tables in the window, and the likely cause as facts: its definition differs, an object it depends on differs (via `INFO.CALCDEPENDENCY`, or by reading the expressions when that isn't available), rows in the window differ (and changed data-source settings), it uses `TODAY()`/`NOW()`, it fails in one model only, or otherwise *unexplained — investigate*.
   - Any difference (or an error in one model only) is **CRITICAL** and becomes a blocker in the report.
- **Step 6 - Server cost**:
   - Traces both models **at the same time** during one single-user pass, with queries alternating A,B / B,A. Both models are therefore measured at the same moment and at the same distance from their warm-up, so neither gets a cache advantage.
   - Only this notebook's sessions are matched, so other users' queries are ignored and never stored.
   - Only the notebook's own traces are dropped afterwards.
- **Step 7 - Load test**:
   - For each level and iteration, rounds alternate A,B → B,A → A,B. This cancels time-of-day and capacity drift.
   - Each virtual user clicks through the report: the queries of one click run in parallel up to the click's captured parallelism, each parallel slot on its own connection; then it waits `think_time_between_actions_sec` before the next click.
   - Peak concurrent queries per model = virtual users × the largest parallelism; preflight prints it and refuses runs above `max_concurrent_queries_guard`.
   - Connections are (re)opened between clicks, with a fresh token before tokens expire, so connecting never counts as waiting time.
   - Cancelling the cell stops all virtual users.

## Reading the report

The report has a short main view for decision makers (the executive summary and one chart) and one collapsed **Details for analysts** section with everything else.

**Main view**

- **Executive summary**. It is built only from the stored results, so it adapts to any pair of models, and it states facts only (no merge decision):
  - **Context**: test time, capacity, load (the rounds actually run per model when the run is incomplete), replay mode, run mode, and the data freshness of both models ("Prod refreshed …; Branch refreshed … (n days older)"). The header above it shows the approval fingerprint.
  - **Blockers** (red, only when something applies): an incomplete run first, in plain words ("cancelled during the load test; 3 of 8 load rounds have results; not measured: the 2 users level"); numbers that differ under the controlled filter (the headline; the measures are in the table below the summary); queries that fail only in B.
  - **Trust: N of M checks passed**, then only the checks that failed (red = not comparable): number check skipped or failed, coverage (executed vs planned load queries and rounds, A,B / B,A balance), at least 3 A/B round pairs per level (fewer: nothing at that level can be clear; set `iterations` ≥ 3), trace match, refresh during the test, capacity, data volume (a table's rows differ by more than 10%), cold start, throttling, other errors under load (by class), round drift (a model's rounds at one level differ by more than 20% in median latency), and the scheduled-run gates. B's data age is in the *Data* line, not a check.
  - **Bottom line**: clear facts first. When single clicks are clearly slower or faster: "By click, Branch is slower on click 1. Refresh (+114%) and faster on click 3. ApplyFilter (−58%); both are clear. Overall (all levels pooled) there is no clear difference: …". When nothing is clear: "No clear speed difference for users (…)". The same for capacity (server CPU), then the number check; for scheduled runs a separate **Trend** line. Then the plain-language KPIs: slowest 10% of visuals, slowest single visual (a single run: never coloured), typical visual, one full click-through, server CPU (drives CU usage) and failed queries (by class). Changes read like "about 2.1× faster (−52%); 9 of 9 round pairs faster"; changes smaller than 100 ms are marked "not noticeable to users".
  - **When is a change clear?** Outside the noise band, its 95% range excludes no change, at least 3 A/B round pairs (an A round and the B round next to it, same level and iteration) of which at least 80% agree in direction, and larger than A's own round-to-round drift. Otherwise it is *not clear* and the reason is given. Fewer than 5 measurements, or no confidence interval, gives no verdict at all (grey).
  - **Pros** (B better) and **Cons** (B worse): only clear changes, with A → B, the 95% range and the round pairs; clearly faster / slower clicks; fewer / more failures. Then which changes are **not clear** (with the reason) or **no meaningful change**, the B-vs-A p90 change per load level ("+3% at 1 user, +25% at 5 users"), and the server CPU in one sentence: net change, largest increase and largest decrease (each from one traced run per query).
  - **Trade-offs and risks**: speed vs capacity (only when both changes are clear and go in opposite directions), changed logic with its number-check outcome, whole-query result differences (split into queries that use a measure that differs in the number check and queries that use only matching measures, plus B's data age), different data volume, different data source or parameters.
  - **Questions to resolve**, **next steps** (for an incomplete run, "run the test again" first), footnotes ¹ ², a collapsed **How to read** note (what counts as clear, what "measured warm" means, how to confirm the exact CU in the Capacity Metrics app), and a **Copy as text** block for email or Teams.
  - **Failed queries**: a query that fails persistently in a model (a query / DAX error, or more than 20% of its runs) is left out of all click metrics, and "fails only in B" is a blocker. A transient failure (throttling, timeout, connection) only drops the click it happened in and that user's click-through; it is counted in the KPI row and the trust line.
- When the number check found differences, a compact table of those measures follows the summary.
- **Wait per click** chart: for each click (user action: page change, filter, slicer, ...), the typical time a user waits under load, A next to B: from the first query start to the last query end of the click. It shows *where* in the report the difference is; only clear changes are coloured.

**Details for analysts** (collapsed; click to open, no re-run needed)

- **Number check**: the settings (columns and how they were found, window and how it was chosen, business units and seed, tolerance), the latest date per table and model, the measures that differ (values, change, where, cause), where they differ (per date for all selected business units, then per business unit for the whole window; measures that differ only because they use another differing measure are named, not repeated), the rows in the window, and the measures that match.
- **At a glance** scorecard: B compared with A for latency (median, p90, p95: all levels pooled; max: single run), click-path wait, error rate, cold first touch, first run, and server CPU/duration.
  - Each metric shows Δ, Δ%, a **95% bootstrap confidence interval** and the **A/B round pairs** (e.g. "4 of 4 round pairs slower; A drift 6%").
  - **Green/red only shows the direction of a clear change**. Grey means no meaningful change or no verdict (single measurement). Amber means not clear, with the reason (CI crosses 0, round pairs disagree or are too few, or the change is within A's round-to-round drift).
- **By concurrency level**: medians and p90 per level, with confidence intervals, round pairs and errors.
- **Server CPU per query** scatter chart: each dot is one query, measured once per model with a single user (so a single dot can be noise). On the dashed line both models use the same CPU; above it B uses more (costs more capacity), below it B uses less. Both axes use a log scale. The 5 largest changes are numbered and listed under the chart.
- **Each visual: how long a user waits**: one row per visual, biggest change first, in plain words:
  - *Typical wait* in A and in B: the middle value of all the runs of that visual's query in the load test (half of the runs were faster, half slower).
  - *Change*: for example "about 5.1× slower (+413%); 9 of 9 round pairs slower", "about the same (−2%)" (within the noise band), or "no clear difference (−21%; only 5 of 9 A/B round pairs agree)". Failed runs are added, e.g. "failed 3× in Branch".
  - *Same numbers?*: whether both models returned the same result for the whole query (Step 4), and whether the query uses a measure that differs in the number check.
- **Errors** (if any), with their class: timeout, throttling, connection, memory or query (a DAX / engine error of the query itself).
- **Data-quality notes**: the long form of the trust line: fewer than 30 samples per level and model, coverage and rounds per model, round drift, incomplete trace, cold start, capacity, last refresh time per model (and a warning if a model was refreshed during the test), incomplete run.

Comparisons saved before the page-load replay existed are shown with their original meaning: the click wait is the sum of the click's query times (sequential replay), and the number check is "not available". Comparisons saved before the fingerprint existed show "-" as fingerprint and are grouped for the trend by query file + models + load shape.

**History** (Step 9): one row per saved comparison, newest first: run mode, fingerprint, query file, settings, the number-check outcome (`match (N)`, `n differ`, `skipped`, `error`, `-` = saved before the number check existed), p90 A and B and the changes, and a note (skip reason, failure, why a run is not comparable). Consecutive skipped scheduled runs are one row ("skipped ×3"). The numbers come from `lt_run_summary`; comparisons without a summary row (saved before it existed, or stopped early) are computed on read and nothing is written back. Then the **trend**: the runs with the same fingerprint as the report (oldest first), with absolute p90 of A and B, a chart, and whether the latest run is above, inside or below the range (min–max) of the last 5 comparable runs. Runs that are not comparable (incomplete, refresh during the test, different capacities, throttling, round drift) are grey and left out of the range. No statistical claim is made; the range needs at least 3 comparable runs.

**Text summary** (Step 10): the executive summary as plain text, then the number check, the model definition differences, the tables whose row count differs, and the queries by latency and by CPU change (one table when there are no more than `top_n_queries`). Copy the whole output to share the exact numbers. Run it after Step 8. After a dry run (`confirm_run = False`) without `report_comparison_id`, it prints a short message instead.

Click-based metrics leave out queries that fail persistently in either model (the number of excluded queries is shown); clicks hit by a transient failure are left out one by one.

## Lakehouse tables

| Table | Grain |
|---|---|
| `lt_comparison` | One row per status change of a run (latest row wins): parameters, models, query file hash, status |
| `lt_query_catalog` | Run × query: action, visual, captured timing, query text |
| `lt_model_profile` | Run × model × item: first touch, object counts, storage modes, definition hashes, rows per table, capacity, last refresh (at start and end), and the number check (below) |
| `lt_executions` | Every query execution (phase `warmup` / `trace` / `load`), timing, rows, checksum, error |
| `lt_server_metrics` | Run × model × query: server duration, CPU, storage-engine queries / CPU / cache matches |
| `lt_result_compare` | Run × query: rows and checksum A vs B |
| `lt_run_summary` | One row per finished (or stopped) comparison, written by the run that made it: `status`, `run_mode`, `fingerprint`, `comparable`, `p50_a/b`, `p90_a/b`, `cpu_a/b`, and `metrics_json` (click-through, first touch, classes, p90 per level, number check, errors by class, failing trust checks, why not comparable, report file). Used for History and the trend |

All tables are keyed by `comparison_id`, so you can also build your own Power BI report on them. New facts go into `parameters_json`, `metrics_json` or new tables, never into new columns, so appends to existing tables keep working.

`lt_comparison.parameters_json` also holds `replay_mode` (`page_load`; comparisons without it are sequential replays), `click_parallelism` (`{action number: parallel queries}`), `approval_fingerprint`, `run_mode` (+ `run_mode_source`, `fabric_context`) and, for scheduled runs, `gates`. Status values: `running`, `completed`, `failed`, `skipped` (scheduled run stopped by a gate; `message` = reason).

Report files (when `save_report_files`): `Files/LoadTest/Reports/<comparison_id>.html` / `.txt` / `.json`.

The number check is stored in `lt_model_profile` with these `item_type` values (rows shared by both models have `model_key = "AB"`; keys with several parts are JSON lists in `item_name`):

| item_type | model_key | item_name | value |
|---|---|---|---|
| `numcheck_config` | AB | `status` (`match` / `differences` / `skipped` / `error`; `value_num` = measures that differ), `reason`, `date_column`, `bu_column` (+ `_source`), `anchor_date`, `end_offset_days`, `anchor_rule`, `window_start`, `window_end`, `bus`, `bu_candidates`, `seed`, `tolerance_pct`, `measures_compared`, `measures_unchecked`, `only_in_A`, `only_in_B`, `query_scoped_skipped`, `dependency_source`, `fact_tables` | `value_text` / `value_num` |
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
- **Number check**: it removes refresh-timing noise only inside the window, and only for tables filtered by the date column. Tables without dates (dimensions, mappings, boundaries/targets) hold their state at the last refresh in each model, so rows added or changed there in Prod after the Branch snapshot change the numbers on every date: the window can't remove that; the *Rows in the window* table shows them in red. Data corrected in place in Prod more than `number_check_end_offset_days` days back, measures that use `TODAY()`/`NOW()`, calculation groups with a default selection, and measures that exist in one model only can still differ, and are reported with that fact. With a different business-unit or date column, or when detection fails, set the override parameters.
- **Cold vs warm**: no cache clearing or forced cold state is used, to avoid hurting production users. Cold behaviour is visible through first touch and the warm-up first run.
- **Refreshes and caches**: a refresh or Direct Lake reframe clears a model's caches. The notebook records each model's last refresh time at the start and the end, and the report flags a model that was refreshed during the test. Repeated identical queries can be served from the engine's cache, so a DAX change that makes queries cache-friendly can look dramatically faster on repeats. Compare the warm-up (first run) numbers with the load-test numbers to see both effects.
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
| Scheduled run `failed`: `Scheduled run not approved: ...` | A load setting or the query file changed (or `schedule_approval` is empty). Run the dry run interactively, paste the printed `schedule_approval = "…"` into Step 1 and save. |
| Scheduled run `skipped`: `no overlapping run ...` | The previous run of the same test is still running, or crashed less than 2× its estimated duration ago (at least 30 min). |
| A scheduled run ran as a dry run, or an interactive run asked for approval | Run-mode detection: set `run_mode` to `"scheduled"` / `"interactive"` and check `parameters_json.fabric_context` of the run. |
| After import, the code of Steps 2–10 is visible | The hidden setting wasn't kept. On each of those code cells: cell `...` menu > *Hide input*, then save the notebook. |

## Maintainers

The `.py` is the source of truth. After editing it, regenerate the notebook and check that both are in sync:

```
python build_notebook.py          # writes ModelCompare_LoadTest_ScheduleRun.ipynb (keeps the cell ids)
python build_notebook.py --check  # exit code 1 if the .ipynb is out of sync
```

The offline harness runs the notebook outside Fabric against fake models with a known, injected truth (B slower on one visual and faster on another, one changed measure, staler data whose newest days are only partly loaded, and per scenario round drift, throttling / timeout errors, B slower only at the top level), and checks that the report shows it. It needs Python 3.11+ with `pandas numpy pyarrow deltalake matplotlib ipython` in a virtual environment outside the repo:

```
python tests/run_harness.py --all --out <folder outside the repo>        # build check + all scenarios (~4 min)
python tests/run_harness.py --scenario confirmed --out <folder>          # one scenario
```

Each scenario writes `steps.txt` (what every step printed), `report.html` (what the notebook shows, charts included), `assertions.txt` and the local lakehouse. Scenarios: `dry_run`, `numcheck_old_window` (the number-check window ending one day before the latest loaded date catches the Branch's partly loaded newest days), `confirmed`, `reopen_by_id`, `reopen_legacy` (a comparison saved by an older version; needs `--legacy`), `interrupted_load`, `load_issues`, `scheduled_bad_fingerprint`, `scheduled_outside_window`, `scheduled_locked`, `scheduled_unreachable_model`, `scheduled_ok`.
