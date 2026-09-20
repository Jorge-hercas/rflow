# Changelog

## Changes

### Bug Fixes

| # | File(s) | Issue | Fix |
| --- | --- | --- | --- |
| 1 | `R/utils.R`, `R/executor.R` | The `{{ ds }}` templating was not applied correctly to `op_args`. | Added `rflow_render_args()`, which renders arguments recursively, including nested lists, before task execution. |
| 2 | `R/executor.R`, `R/task.R` | `timeout` was stored but **never enforced**. | Implemented using `setTimeLimit()`. Documented that it cannot interrupt `Sys.sleep()` or blocking system/network calls—a limitation of base R. |
| 3 | `R/scheduler.R` | An unexpected error (for example, a failed database connection) **stopped the entire scheduler loop**. | Wrapped `run_dag()` in `tryCatch` per DAG and per tick; the loop remains active and logs the error. |
| 4 | `R/scheduler.R` | `catchup` was referenced incorrectly. | Added `.rflow_due_execution_dates()`: with `catchup = TRUE`, all missed intervals are triggered; with `FALSE`, only the most recent one is triggered. |
| 5 | `R/dag.R` | DAG `default_args` **always overrode** explicit task values, even when passing `retries = 7`. | Added the `.explicit_args` flag system: explicit values in `r_task()` now always take precedence, with Airflow-like semantics. |
| 6 | `R/ui.R` | An `NA` log was displayed as the literal text `"NA"` instead of `"(no output)"`. | Missing logs are now displayed as `"(no output)"`. |
| 7 | `R/ui.R` | The dashboard’s **Trigger run** button blocked the entire UI while the DAG was running. | Added background execution via `future`, with synchronous fallback when no plan is configured or the database is `:memory:`; a single poller prevents observer leaks. |

### New Features

| # | File(s) | Added functionality |
| --- | --- | --- |
| 8 | `R/task.R` | `on_success_callback` and `on_failure_callback` in `r_task()`: hooks for notifications or alerts, with their own error handling. |
| 9 | `R/db.R` | `rflow_db_connect()` now accepts an already-open `DBIConnection` (Postgres, MySQL, etc.), not just a SQLite path. |
| 10 | `R/db.R` | Replaced SQLite-specific `INSERT OR REPLACE` with standard `INSERT ... ON CONFLICT ... DO UPDATE` syntax. |

### General Library Changes

Changes identified while running `R CMD check --as-cran`.

| # | File(s) | Fix |
| --- | --- | --- |
| 11 | `DESCRIPTION` | Removed the unused `digest` dependency. |
| 12 | `R/dag.R` | Added missing `importFrom` declarations for `setNames` (`stats`) and `modifyList` (`utils`). |
| 13 | `R/executor.R` | Fixed the false-positive global-variable warning caused by `textConnection("log_capture", ...)`. |
| 14 | `DESCRIPTION` | `License: MIT` now includes `+ file LICENSE`; the description no longer starts with the package name. |
| 15 | `R/executor.R` | Documented the `run_type` parameter of `run_dag()`. |
| 16 | `.Rbuildignore` | Added `.github/` and `examples/`, which generated NOTES during package checks. |
| 17 | `NEWS.md` | Created the file using a parseable format. |

### New Unit Tests

- **24 new tests**: increased from 20 to 44.
  - `tests/testthat/test-executor-fixes.R`: templating, timeout, `default_args`, and callbacks.
  - `tests/testthat/test-scheduler-fixes.R`: catchup behavior and error resilience.
  - `tests/testthat/test-db-fixes.R`: external connections and portable upserts.
- `.github/workflows/R-CMD-check.yaml`: runs `R CMD check` on Linux, macOS, and Windows, using two R versions.
- `.github/workflows/test-coverage.yaml`: measures coverage with `covr` and publishes it to Codecov.