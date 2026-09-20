#' Compute the next scheduled execution time for a DAG given the last run
#' @param dag A [DAG] object.
#' @param after A POSIXct time to compute the next run after (default: last
#'   recorded dag_run's execution_date from `con`, or `dag$start_date`).
#' @param con Optional DBI connection to look up the last run.
#' @export
next_run_time <- function(dag, after = NULL, con = NULL) {
  step <- rflow_schedule_to_seconds(dag$schedule_interval)
  if (is.na(step)) return(NA)
  if (is.null(after)) {
    after <- as.POSIXct(dag$start_date)
    if (!is.null(con)) {
      runs <- rflow_db_runs(con, dag$dag_id)
      if (nrow(runs) > 0) after <- as.POSIXct(max(runs$execution_date), tz = "UTC")
    }
  }
  as.POSIXct(after) + step
}

#' Compute every scheduled execution date that is currently due for a DAG
#'
#' Honors `dag$catchup`: when `TRUE`, every missed interval between the last
#' run (or `start_date`) and now is returned, so [scheduler_run()] can
#' materialize all of them (like `airflow dags backfill` running
#' automatically). When `FALSE` (the default), only the single most recent
#' due interval is returned and earlier missed intervals are skipped -- this
#' is what actually makes the `catchup` flag documented on [DAG] do anything.
#'
#' @param dag A [DAG] object.
#' @param con Optional DBI connection to look up the last run.
#' @param now The current time to compare against (default `Sys.time()`).
#' @return A (possibly empty) vector of POSIXct execution dates, oldest first.
#' @keywords internal
.rflow_due_execution_dates <- function(dag, con = NULL, now = Sys.time()) {
  step <- rflow_schedule_to_seconds(dag$schedule_interval)
  if (is.na(step)) return(as.POSIXct(character(0)))
  last <- as.POSIXct(dag$start_date)
  if (!is.null(con)) {
    runs <- rflow_db_runs(con, dag$dag_id)
    if (nrow(runs) > 0) last <- as.POSIXct(max(runs$execution_date), tz = "UTC")
  }
  now <- as.POSIXct(now)
  first_due <- last + step
  if (first_due > now) return(as.POSIXct(character(0)))
  if (isTRUE(dag$catchup)) {
    seq(first_due, now, by = step)
  } else {
    n_missed <- floor(as.numeric(difftime(now, first_due, units = "secs")) / step)
    first_due + n_missed * step
  }
}

#' Run a simple in-process scheduler loop (a minimal stand-in for the Airflow
#' scheduler + executor daemons)
#'
#' Polls a set of DAGs and triggers a [run_dag()] whenever a DAG's
#' `schedule_interval` has elapsed since its last recorded run. Respects each
#' DAG's `catchup` flag: with `catchup = TRUE` every missed interval since the
#' last run is triggered in order; with `catchup = FALSE` (default) only the
#' latest due interval is triggered and older missed intervals are skipped.
#' This is a single-process, blocking loop intended for local development or
#' simple always-on R sessions (e.g. inside a Docker container or `Rscript`
#' cron job) — not a distributed scheduler.
#'
#' An error raised by an individual [run_dag()] call (as opposed to a task
#' failing, which is already handled via retries/trigger_rule) is caught,
#' logged to the console, and the loop keeps polling rather than crashing --
#' a scheduler that dies on the first hiccup defeats the point of a
#' long-running daemon.
#'
#' @param dags A list of [DAG] objects to schedule.
#' @param con A DBI connection from [rflow_db_connect()] used to persist runs
#'   and track "last run" state across ticks/restarts.
#' @param poll_interval Seconds to sleep between checks (default 5).
#' @param max_ticks Optional integer cap on the number of polling iterations,
#'   useful for tests/demos so the loop terminates; NULL runs forever.
#' @param executor "sequential" or "future", passed through to [run_dag()].
#' @export
scheduler_run <- function(dags, con, poll_interval = 5, max_ticks = NULL, executor = "sequential") {
  if (inherits(dags, "DAG")) dags <- list(dags)
  tick <- 0
  repeat {
    tick <- tick + 1
    now <- Sys.time()
    for (dag in dags) {
      due <- tryCatch(
        .rflow_due_execution_dates(dag, con = con, now = now),
        error = function(e) {
          cat(sprintf("[scheduler] %s: error computing due runs, skipping this tick: %s\n",
                      dag$dag_id, conditionMessage(e)))
          as.POSIXct(character(0))
        }
      )
      for (ed in due) {
        cat(sprintf("[scheduler] %s: due (execution_date=%s <= now=%s), triggering run\n",
                    dag$dag_id, format(ed), format(now)))
        tryCatch({
          run_dag(dag, execution_date = ed, executor = executor, con = con, run_type = "scheduled")
        }, error = function(e) {
          cat(sprintf("[scheduler] %s: run_dag() raised an unexpected error, skipping: %s\n",
                      dag$dag_id, conditionMessage(e)))
        })
      }
    }
    if (!is.null(max_ticks) && tick >= max_ticks) break
    Sys.sleep(poll_interval)
  }
  invisible(NULL)
}
