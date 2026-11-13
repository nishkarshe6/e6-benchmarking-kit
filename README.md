# Benchmarking script for e6data and Databricks

`run_queries.py` runs the SQL queries in a CSV file on e6data, on a Databricks SQL warehouse, or on both, one at a time or on a fixed load pattern (`--pattern`). It writes every execution to a results CSV, with the time measured by the script and each engine's own server-side timings.


> **Credentials.** The script logs in as you, with your own e6data access token (Quick start, step 3).

## Before you start: turn on the sandbox

At handover the workspace, the warm pool and the `sandbox` cluster are switched off to save cost. Turn them on in this order at the start of each benchmark session:

1. **Workspace:** in the e6data console (app.e6.run), open **Workspaces** and find `infosys-sandbox`. If its Status is `Disabled`, click the play (triangle) button in its Actions column and confirm; wait until it shows `Running`. Then click **Open** to go into the workspace.
2. **Warm pool:** in the workspace, click **Warm Pools** in the left menu. On the `sandbox-warmpool` row, click the play (triangle) button (Resume warm pool) and wait until Status shows `Running`. It keeps machines ready so the cluster resumes in about a minute; without it, resuming takes longer and the first timings of a session are slower.
3. **Cluster:** click **Clusters** in the left menu. On the `sandbox` row (ignore any other cluster), click the play (triangle) button and wait until Status shows `Running`. Or just run the script: it resumes the cluster when it logs in and waits until the cluster is ready before it starts timing.

When you finish for the day, suspend the `sandbox` cluster and the `sandbox-warmpool` warm pool: click the two-bar (pause) button on each row. The cluster also suspends by itself after 15 minutes without queries.

## Quick start (e6data)

1. **Turn on the sandbox** ([Before you start](#before-you-start-turn-on-the-sandbox)).
2. **Install Python and the e6data connector** (one time; Windows: see [section 1](#1-setup-one-time)):

   ```
   python3 -m venv .venv
   ```

   ```
   source .venv/bin/activate
   ```

   ```
   pip install e6data-python-connector==3.0.0
   ```

3. **Create your access token and set it in your terminal** (one time). In the e6data console, open the `infosys-sandbox` workspace, then the avatar menu (top right) > **User settings** > **Access tokens** > **Create**. Copy the token straight away; it starts with `e6pat_` and is shown only once. Then set it with your e6data login email (Windows and details: [Your access token](#your-access-token-pat)):

   ```
   export E6_USER="you@infosys.com"
   ```

   ```
   export E6_TOKEN="e6pat_..."
   ```

4. **Run** the included example queries, or your own CSV ([section 2](#2-prepare-the-queries-csv)):

   ```
   python run_queries.py example_queries.csv --runs 3
   ```

The last lines of the output show what the run cost on e6data: core-hours and dollars for the `sandbox` cluster while the run was going ([Usage and cost](#usage-and-cost)).

To run the same queries on Databricks, see [section 3.1](#31-run-on-databricks).

## 1. Setup (one time)

You need Python 3.9 or newer (from python.org, or your package manager).

macOS / Linux:

```
python3 -m venv .venv
source .venv/bin/activate
pip install e6data-python-connector==3.0.0
```

Windows (PowerShell):

```
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install e6data-python-connector==3.0.0
```

If PowerShell refuses to run `Activate.ps1`, first run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

The connection details (host, cluster `sandbox`, catalog `demo`) are at the top of `run_queries.py`. You log in with your own access token (Quick start, step 3; details in [Your access token](#your-access-token-pat)). The machine needs outbound HTTPS (port 443). Behind a corporate proxy, set the `https_proxy` environment variable.

### Your access token (PAT)

The script logs in with your own personal access token (PAT), so your runs also show in the e6data console's **Query History** (with the **Viewer** role you see only your own queries there).

Create a PAT (one time):

1. Sign in to the e6data console and open the `infosys-sandbox` workspace.
2. Open the avatar menu (top right), then **User settings**.
3. Open the **Access tokens** tab and select **Create**.
4. Enter a comment (3 to 200 characters), for example `benchmark script`.
5. Set **Token Expiry** in days (default 90). Don't tick "Never expires".
6. Select **Create** and copy the token straight away (it starts with `e6pat_`). It's shown only once; if you lose it, create a new one and delete the old one.

Then set two environment variables in the same terminal before running the script. Use your e6data login email and your PAT.

macOS / Linux:

```
export E6_USER="you@infosys.com"
export E6_TOKEN="e6pat_..."
```

Windows (PowerShell):

```
$env:E6_USER = "you@infosys.com"
$env:E6_TOKEN = "e6pat_..."
```

The `e6data:` line printed just below `N queries x N run(s)` ends with `as <user>`, which tells you who it's running as. The script stops if either variable is missing.

In the console, open **Query History** and filter by your email, or search by the `query_id` from the results CSV. Your PAT has your role's permissions, and it stops working when your account is removed. Treat it like a password.

## 2. Prepare the queries CSV

SQL reference: [e6data SQL functions](https://docs.e6data.com/query/sql/functions/index).

Any CSV with one column of SQL queries works; the column names don't matter. The script finds the SQL column by its content (the cells that start with SELECT, WITH and so on), and takes query names from a column whose header contains name, id, label, key or title. Without one, queries are named q001, q002, and so on. It prints which columns it used. `example_queries.csv` is a small example to start from.

```
name,sql
store_sales_count,select count(*) from store_sales
sales_by_year,"select d_year, count(*) from store_sales join date_dim on ss_sold_date_sk = d_date_sk group by d_year"
```

- Wrap a query in double quotes if it contains commas or line breaks. Excel and Google Sheets do this automatically when you save as CSV.
- Comma, semicolon and tab separated files all work. UTF-8 is preferred.
- If more than one column holds SQL (for example one per engine), the first is used and the others are listed. Pick another with `--sql-column` (a header or a column number, counting from 1), and the name column with `--name-column`.
- Use plain table names (`store_sales`). They're resolved in the dataset you choose with `--schema`.
- Put one statement per row. A trailing `;` is fine. Rows that are empty or only `--` comments are skipped.
- A file with only a `sql` column (or no header, just one query per line) is fine too; commas inside the SQL are kept.
- If two rows share a name, the second is reported as `name#2`.

## 3. Run

Run from the folder that contains `run_queries.py`:

```
python run_queries.py example_queries.csv
```

| Option | Meaning | Default |
|---|---|---|
| `--runs N` | Run each query N times | 1 |
| `--pattern FILE` | Start queries in steps of different rates: a JSON list of `{"Seconds": S, "QPS": N}` steps, run in order, cycling through the CSV (see below) | off |
| `--max-inflight N` | With `--pattern`: the most queries this machine keeps open at once | 64 |
| `--cluster NAME` | e6data cluster to run on | `sandbox` |
| `--schema NAME` | Dataset: `tpcds_sf1000_delta`, TPC-DS 1 TB (24 tables) | `tpcds_sf1000_delta` |
| `--out FILE` | Results file | `e6_results_<date>_<time>.csv` |
| `--sql-column NAME` | Column holding the SQL (header or number, from 1) | found automatically |
| `--name-column NAME` | Column holding query names | found automatically |
| `--no-engine-stats` | Skip fetching e6data's server-side timings (saves one short call per query) | timings on |
| `--engine NAME` | `e6data`, `databricks`, or `both` (same queries on the same schedule, see 3.1) | `e6data` |
| `--dbr-host`, `--dbr-http-path`, `--dbr-catalog`, `--dbr-schema` | Databricks workspace host, SQL warehouse HTTP path, and the catalog and schema holding the same tables | `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CATALOG`, `DATABRICKS_SCHEMA` |

Example, each query 3 times:

```
python run_queries.py example_queries.csv --runs 3 --out results_3runs.csv
```

Load pattern: the included `qps_pattern.json` holds these steps (it starts 275 queries over 98 s), then run with `--pattern`:

```
[
  {"Seconds": 10, "QPS": 2},
  {"Seconds": 8, "QPS": 5},
  {"Seconds": 5, "QPS": 1},
  {"Seconds": 5, "QPS": 4},
  {"Seconds": 15, "QPS": 2},
  {"Seconds": 15, "QPS": 4},
  {"Seconds": 20, "QPS": 2},
  {"Seconds": 10, "QPS": 4},
  {"Seconds": 10, "QPS": 2}
]
```

```
python run_queries.py example_queries.csv --pattern qps_pattern.json
```

`--pattern` starts queries on a clock, whether or not earlier ones have finished, so if the cluster falls behind, queries pile up (queue) on the cluster, as they would with real users. At the end the script prints the arrivals line: queries started, how late they started, the peak number running on the cluster, and, when arrivals stopped, how many were running and how many were still waiting for a connection. The summary file records the same. The script never has more than `--max-inflight` queries on the cluster at once, so if start delays are above 1 s the cluster saw at most that many at once rather than the full rate; add `--max-inflight 200` and rerun.

Press Ctrl+C to stop. The script cancels the running queries on the cluster and waits up to 30 s; those that stop in time are written as `CANCELLED`, and any still running after that are counted as "not run". Press Ctrl+C a second time to quit at once; queries already sent then finish or time out on the cluster. Everything completed so far stays in the results file.

For throughput runs, add `--no-engine-stats`: fetching the server timings adds a short call after every query, which lowers queries per second.

### 3.1 Run on Databricks

`--engine databricks` runs the queries on a Databricks SQL warehouse instead of e6data. `--engine both` sends every query to both together. Both engines get the same queries in the same order, starting at the same moment. With `--pattern` every query also starts at the same time on both (within a few ms). Each engine has its own connections, so a slow engine never holds up the other. With `--runs` each engine works through the list at its own pace.

Point it at **your own** Databricks workspace and SQL warehouse, with your own copy of the same tables. The script asks for the host, HTTP path, catalog, schema and token if they aren't set.

One-time setup, in the same virtual environment:

```
pip install databricks-sql-connector
```

Point it at a warehouse and at the catalog and schema that hold the same tables (plain table names in the CSV are resolved there). Find the host and HTTP path under **SQL Warehouses**, then your warehouse, then **Connection details**:

```
export DATABRICKS_HOST="dbc-xxxx.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<id>"
export DATABRICKS_CATALOG="<catalog>"
export DATABRICKS_SCHEMA="<schema>"
```

Windows: `$env:DATABRICKS_HOST = "..."` and so on. Any value not set is asked for when the script starts.

The Databricks personal access token is asked for at the start (typing is hidden). To skip the prompt, set `DATABRICKS_TOKEN`. The token is never printed or written to a file.

```
python run_queries.py example_queries.csv --engine databricks
```

- Each Databricks connection turns the result cache off (`SET use_cached_result = false`). If that is refused, the script stops before running anything.
- Logging in starts a stopped warehouse; the script waits up to 10 minutes for it.
- After the run, Databricks' own timings are read from its Query History API (`/api/2.0/sql/history/queries`). If the token can't read query history, those columns stay blank and a note says so.
- Ctrl+C cancels the running queries on both engines.
- `dbr_total_ms` (summary: `databricks_ms`) is Databricks' own time for the query, measured on the server, like `e6_total_ms` for e6data.

## 4. Results

At the end the script prints:
- a table per query: successful runs, then median, min and max time measured by the script, and e6data's median server time;
- an overall line: queries per second (successful only), and latency p50, p95, max and mean across all successful executions. The percentiles use linear interpolation, the same as Excel PERCENTILE.INC.

The exit code is 0 if every execution succeeded, otherwise 1.

Two files are written:
- the results file (`--out`), with one row per execution (columns below);
- a summary file next to it, named `<results name>_summary.csv`, for the whole run, in milliseconds over successful executions:
  - one row per statistic: p50, p75, p90, p99, max, mean and total;
  - one column per time: `e6_client_ms` (measured by the script), `e6_total_ms`, `e6_execution_ms`, `e6_queue_ms` and `e6_planning_ms` (e6data's own times);
  - with `--engine databricks` or `both`, `databricks_ms`, `databricks_client_ms`, `databricks_execution_ms` and `databricks_compile_ms`;
  - underneath, the SUCCESS / FAILED / CANCELLED / NOT_RUN counts, the dataset and the run mode (wall time and throughput are printed in the terminal).

  Each percentile is computed per column. The e6data server columns are empty with `--no-engine-stats`.

The results CSV has one row per execution:

| Column | Meaning |
|---|---|
| `name`, `run` | Query name and run number |
| `status` | `OK`, `FAILED`, or `CANCELLED` (stopped with Ctrl+C) |
| `client_seconds` | Time measured by the script: sending the query, running it, and fetching all result rows over the network |
| `rows` | Rows returned |
| `e6_planning_ms`, `e6_queue_ms` | e6data planning and queueing time |
| `e6_execution_ms` | e6data execution time |
| `e6_total_ms` | e6data's end-to-end time for the query, measured on the server |
| `query_id` | e6data query id, for the e6data team to look up |
| `error` | Error message for failed queries, cut to 1,000 characters |

With `--engine databricks` or `both`, these columns are added:

| Column | Meaning |
|---|---|
| `engine` | `e6data` or `databricks` |
| `started` | When the query was sent (local time, ms) |
| `dbr_total_ms` | Databricks' own duration for the query (also the summary's `databricks_ms`) |
| `dbr_execution_ms`, `dbr_compile_ms` | Databricks execution and compilation time |
| `dbr_rows_read`, `dbr_cached` | Rows Databricks read, and whether the answer came from its cache |

For Databricks rows, `query_id` is the Databricks statement id.

### Usage and cost

Before the clock starts, the script runs one untimed warm-up query and waits until the cluster is at its usual size (up to 5 minutes; after a resume it first waits 1 minute).

At the end of every run the script reads e6data's own usage view, `system.usage.core_usage`, and prints the core-hours and cost of the cluster for the time the run was going (each usage interval is counted only for the part inside the run). The same figures are in the summary file as `e6_core_hours` and `e6_cost_usd`. It is the whole cluster's usage, so if others query the same cluster at the same time, their share is included. New usage can take a few minutes to appear after the start of each hour; the script waits up to about 4 minutes for it, and if usage is still missing it prints a warning that the figure is low; rerun the SQL below a few minutes later for the full number.

To look at usage yourself, run this in the e6data SQL editor:

```
SELECT * FROM system.usage.core_usage WHERE cluster_name = 'sandbox' ORDER BY active_start DESC
```

### Timing notes

- `client_seconds` includes the network between your machine and e6data: each query costs a few round trips. From a machine far from AWS us-east-1, each query takes about 1 s longer than the server time. For server-side time, use `e6_total_ms`, or run the script from a machine in AWS us-east-1.
- Fetching very large results adds transfer time to `client_seconds`. Prefer aggregates or `LIMIT` for timing runs.
- The first run of a query after a period of inactivity is usually slower, because table metadata is loaded. Run with `--runs 2` or more, and report run 1 (cold) and runs 2 onward (warm) separately; the `run` column in the results file tells them apart. The summary file covers all runs together. For a warm-only summary, run once to warm up, then run again with a new `--out`.
- On Databricks the script turns the result cache off (`SET use_cached_result = false`), so every query really runs.

## 5. Troubleshooting

| Message | What to do |
|---|---|
| `could not reach ... within 420s` | Outbound HTTPS (port 443) is blocked, or a proxy is needed. Set `https_proxy` or try another network |
| `login failed` / `PERMISSION_DENIED` | Check that `E6_USER` is your login email (not a display name), that the token hasn't expired, and that your user has a role on the `sandbox` cluster and `demo` catalog |
| `the cluster is suspended and could not be resumed from here` | The cluster pauses after 15 minutes idle, and the script normally resumes it at login (a few minutes). If that fails, ask the e6data team to resume `sandbox`, then rerun |
| `{INVALID QUERY} ... [line x, col y]` | SQL syntax error, or an unknown table or column. Check the dataset chosen with `--schema` |
| Queries on `information_schema` fail | Known issue on this workspace. Use `SHOW TABLES` and `DESCRIBE <table>` instead |
| Queries stop at 900 s | The cluster's query timeout is 900 s |
| A query seems stuck well past 900 s | The network connection probably dropped (the script has no deadline of its own). Press Ctrl+C and rerun |
| `cannot write ...` | The results file is open in Excel or the folder is read-only. Close it or pass another `--out` |

## Security and support

Your access token works like a password: keep it to yourself, set it as an environment variable each time (never in a file or script), and delete it under **User settings > Access tokens** when the evaluation ends. Never commit it to a code repository.

For help, send the e6data team the `query_id` and the error.
