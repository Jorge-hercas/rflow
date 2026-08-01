#' Trigger a single ad-hoc run of a DAG (alias for [run_dag()])
#' @inheritParams run_dag
#' @export
trigger_dag <- function(dag, execution_date = Sys.time(), executor = "sequential", con = NULL, conf = list()) {
  run_dag(dag, execution_date = execution_date, executor = executor, con = con, conf = conf, run_type = "manual")
}

#' Test a single task in isolation, outside of a full DAG run
#'
#' Analogous to `airflow tasks test`. Runs one task's function directly with a
#' synthetic context, ignoring upstream dependency state (useful for quick
#' iteration while authoring a task).
#'
#' @param dag A [DAG] object.
#' @param task_id The task to test.
#' @param execution_date Logical execution date for templating (default now).
#' @param ti_overrides Named list to inject as `context$ti`, simulating
#'   upstream task results without actually running them.
#' @export
test_task <- function(dag, task_id, execution_date = Sys.time(), ti_overrides = list()) {
  task <- dag$get_task(task_id)
  ctx <- rflow_macro_context(execution_date, dag_id = dag$dag_id, task_id = task_id,
                              run_id = "test", extra = list(ti = ti_overrides))
  out <- .run_single_task(task, ctx, ti_overrides)
  cat(out$log, "\n")
  if (out$state == "failed") stop("rflow: task '", task_id, "' failed: ", out$error)
  invisible(out$result)
}

#' Print a summary table of DAG runs
#' @param con connection from [rflow_db_connect()]
#' @param dag_id optional filter
#' @export
list_runs <- function(con, dag_id = NULL) {
  runs <- rflow_db_runs(con, dag_id)
  if (nrow(runs) == 0) {
    cat("(no runs yet)\n")
    return(invisible(runs))
  }
  print(runs[, c("run_id", "dag_id", "execution_date", "state", "run_type")])
  invisible(runs)
}

#' Print a Gantt-style text summary of a single DAG run's task instances
#' @param con connection from [rflow_db_connect()]
#' @param run_id the run to summarize
#' @export
show_run <- function(con, run_id) {
  ti <- rflow_db_task_instances(con, run_id)
  if (nrow(ti) == 0) {
    cat("(no task instances found for run_id '", run_id, "')\n", sep = "")
    return(invisible(ti))
  }
  cat(sprintf("Run: %s\n", run_id))
  for (i in seq_len(nrow(ti))) {
    r <- ti[i, ]
    cat(sprintf("  %-25s %-8s try=%d/%d  %6.3fs  %s\n",
                r$task_id, r$state, r$try_number, r$max_tries,
                r$duration %||% NA, ifelse(is.na(r$error), "", r$error)))
  }
  invisible(ti)
}
