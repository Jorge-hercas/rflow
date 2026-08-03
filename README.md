# rflow

**A native R implementation of Apache Airflow.** A lightweight workflow orchestrator, written entirely in R, inspired by the architecture of [Apache Airflow](https://github.com/apache/airflow) — but using R idioms instead of Python decorators.

This is not a Python wrapper via `reticulate`. Instead, i did a reimplementation of Airflow's core concepts (DAG, Task/Operator, Executor, Scheduler, metadata DB, XCom, webserver) using only tools from the R ecosystem.

## Installation

```r
install.packages(c("R6", "DBI", "RSQLite", "jsonlite", "future", "future.apply", "uuid", "digest", "shiny"))
devtools::install_github("Jorge-hercas/rflow")
```

## Airflow equivalents

| Airflow (Python)                                   | rflow (R)                                     |
|-----------------------------------------------------|------------------------------------------------|
| `with DAG(...) as dag:`                              | `with_dag(DAG$new(...), { ... })`               |
| `PythonOperator(task_id=..., python_callable=...)`   | `r_task(task_id, function(...) ...)`            |
| `task1 >> task2`                                     | `task1 %>>% task2`                              |
| `task2 << task1`                                     | `task2 %<<% task1`                              |
| XCom (`ti.xcom_pull()`)                              | `context$ti$task_name` (return value)           |
| Metadata DB (Postgres/MySQL)                          | SQLite via `rflow_db_connect()`                 |
| Scheduler daemon                                      | `scheduler_run()`                               |
| `airflow dags trigger`                                | `trigger_dag()` / `run_dag()`                   |
| `airflow dags backfill`                               | `backfill()`                                    |
| `airflow tasks test`                                  | `test_task()`                                   |
| Webserver (UI)                                        | `rflow_ui()` (Shiny dashboard)                  |
| `retries`, `retry_delay`                              | identical                                       |
| `trigger_rule`                                        | identical (`all_success`, `all_done`, `one_success`, `one_failed`, `none_failed`) |
| Jinja templating (`{{ ds }}`)                         | `rflow_render()` with the same `{{ ds }}` syntax |

## Quick example

```r
library(rflow)

dag <- with_dag(DAG$new("etl_demo", schedule_interval = "@daily"), {
  extract <- r_task("extract", function() list(a = 1, b = 2, c = 3))

  transform_a <- r_task("transform_a", function(context) sum(unlist(context$ti$extract)) * 2)
  transform_b <- r_task("transform_b", function(context) max(unlist(context$ti$extract)))

  load <- r_task("load", function(context) context$ti$transform_a + context$ti$transform_b)

  extract %>>% list(transform_a, transform_b) %>>% load
})

con <- rflow_db_connect("rflow.db")
result <- run_dag(dag, con = con) # sequential execution
result <- run_dag(dag, executor = "future") # parallel execution, layer by layer

list_runs(con, "etl_demo")
show_run(con, result$run_id)

rflow_ui(dag, con = con)   # interactive dashboard (Shiny)
```

## Features

- **DAGs built with R6**: declarative definition of tasks and dependencies.
- **`%>>%` / `%<<%` operators**: chainable, with fan-out/fan-in support (`extract %>>% list(a, b) %>>% load`).
- **Native XCom**: any task's return value is automatically available at `context$ti$<task_id>` for downstream tasks — no manual serialization required.
- **Automatic retries** per task (`retries`, `retry_delay`).
- **Full `trigger_rule` support**: `all_success`, `all_done`, `one_success`, `one_failed`, `none_failed`.
- **Cycle detection** and DAG validation before execution.
- **Two executors**: `sequential` and `future` (real parallelism across topological layers using the `future` package).
- **SQLite persistence**: every run (`dag_run`) and every task (`task_instance`) is recorded with state, attempt count, duration, logs, and result — just like Airflow's metadata database.
- **Simple scheduler** (`scheduler_run()`): a polling loop that triggers runs according to `schedule_interval` (`@hourly`, `@daily`, `"5 mins"`, etc).
- **Backfill** (`backfill()`): materializes historical runs between two dates.
- **`{{ ds }}` templating**: Airflow-style Jinja-like execution-date macros.
- **Shiny dashboard** (`rflow_ui()`): visualizes the dependency graph (dynamically generated SVG, "transit map" style), run history, per-task state, and logs — all without leaving R.

## Current limitations

- The scheduler (`scheduler_run()`) is a single-process R polling loop, not a distributed daemon with task queues like Airflow's real scheduler.
- No authentication/RBAC in the dashboard (intended for local/internal use).
- "Operators" are simply R functions; there are no providers/hooks for external services (S3, BigQuery, etc.) — but any R function (including calls to packages like `aws.s3`, `bigrquery`, `DBI`, `httr2`) works fine as a task's body.
