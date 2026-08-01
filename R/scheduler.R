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

#' Run a simple in-process scheduler loop (a minimal stand-in for the Airflow
#' scheduler + executor daemons)
#'
#' Polls a set of DAGs and triggers a [run_dag()] whenever a DAG's
#' `schedule_interval` has elapsed since its last recorded run. This is a
#' single-process, blocking loop intended for local development or simple
#' always-on R sessions (e.g. inside a Docker container or `Rscript` cron
#' job) — not a distributed scheduler.
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
      nrt <- next_run_time(dag, con = con)
      if (!is.na(nrt) && now >= nrt) {
        cat(sprintf("[scheduler] %s: due (next_run_time=%s <= now=%s), triggering run\n",
                    dag$dag_id, format(nrt), format(now)))
        run_dag(dag, execution_date = now, executor = executor, con = con, run_type = "scheduled")
      }
    }
    if (!is.null(max_ticks) && tick >= max_ticks) break
    Sys.sleep(poll_interval)
  }
  invisible(NULL)
}
