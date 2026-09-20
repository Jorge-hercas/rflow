#' @keywords internal
.eval_trigger_rule <- function(rule, upstream_states) {
  if (length(upstream_states) == 0) return(TRUE)  # root task
  switch(rule,
    all_success = all(upstream_states == "success"),
    all_done    = all(upstream_states %in% c("success", "failed", "skipped")),
    one_success = any(upstream_states == "success"),
    one_failed  = any(upstream_states == "failed"),
    none_failed = !any(upstream_states == "failed"),
    stop("rflow: unknown trigger_rule '", rule, "'")
  )
}

#' @keywords internal
.run_single_task <- function(task, context, xcom_store) {
  attempt <- 0
  max_tries <- task$retries + 1
  last_error <- NULL
  log_lines <- character(0)
  start_date <- Sys.time()

  while (attempt < max_tries) {
    attempt <- attempt + 1
    log_capture <- NULL  # populated as a side effect of the textConnection() below
    tc <- textConnection("log_capture", "w", local = TRUE)
    sink(tc, split = FALSE)
    result <- NULL
    ok <- TRUE
    t0 <- Sys.time()
    err_msg <- NA_character_
    if (!is.null(task$timeout)) {
      on.exit(setTimeLimit(cpu = Inf, elapsed = Inf, transient = TRUE), add = TRUE)
      setTimeLimit(elapsed = task$timeout, transient = TRUE)
    }
    tryCatch({
      call_args <- rflow_render_args(task$op_args, context)
      fmls <- names(formals(task$func))
      if ("context" %in% fmls) call_args$context <- context
      result <- do.call(task$func, call_args)
    }, error = function(e) {
      ok <<- FALSE
      err_msg <<- conditionMessage(e)
    })
    if (!is.null(task$timeout)) setTimeLimit(cpu = Inf, elapsed = Inf, transient = TRUE)
    sink()
    close(tc)
    log_lines <- c(log_lines, sprintf("[attempt %d/%d] %s", attempt, max_tries, format(t0)),
                    log_capture)
    if (ok) {
      return(list(state = "success", result = result, error = NA_character_,
                  try_number = attempt, max_tries = max_tries,
                  start_date = start_date, end_date = Sys.time(),
                  log = paste(log_lines, collapse = "\n")))
    } else {
      last_error <- err_msg
      log_lines <- c(log_lines, sprintf("ERROR: %s", err_msg))
      if (attempt < max_tries) {
        log_lines <- c(log_lines, sprintf("Retrying in %ss...", task$retry_delay))
        Sys.sleep(task$retry_delay)
      }
    }
  }
  list(state = "failed", result = NULL, error = last_error,
       try_number = attempt, max_tries = max_tries,
       start_date = start_date, end_date = Sys.time(),
       log = paste(log_lines, collapse = "\n"))
}

#' @keywords internal
.run_callback <- function(callback, context, payload) {
  tryCatch(
    callback(context, payload),
    error = function(e) {
      warning("rflow: callback raised an error and was ignored: ", conditionMessage(e), call. = FALSE)
    }
  )
  invisible(NULL)
}

#' Execute a DAG once (a "DAG run"), analogous to `airflow dags trigger`
#'
#' Runs every task in dependency order, honoring retries, `trigger_rule`s, and
#' persisting state to the metadata DB if `con` is supplied. Task return
#' values are collected into `context$ti`, a named list keyed by `task_id` —
#' R's answer to Airflow's XCom — so downstream tasks can read upstream
#' results simply as `context$ti$upstream_task_id`.
#'
#' @param dag A [DAG] object.
#' @param execution_date The logical execution date/time for this run
#'   (defaults to now). Used to populate `{{ ds }}`-style template macros.
#' @param executor "sequential" (default) or "future" for layer-parallel
#'   execution via the future package (configure a plan with
#'   `future::plan(future::multisession)` beforehand for real parallelism).
#' @param con Optional DBI connection from [rflow_db_connect()] to persist run
#'   and task_instance state. If NULL, runs in-memory only.
#' @param run_id Optional explicit run id; auto-generated (UUID) if omitted.
#' @param conf Optional named list of extra config, merged into the template
#'   context and stored alongside the dag_run row.
#' @param verbose Print a live progress summary to the console (default TRUE).
#' @param run_type Free-text label stored on the dag_run row, e.g. `"manual"`
#'   (the default, used by [trigger_dag()]) or `"scheduled"` (used
#'   internally by [scheduler_run()]/[backfill()]). Purely informational.
#' @return An invisible list with `run_id`, `state` ("success"/"failed"),
#'   `results` (named list of task return values), and `task_instances`
#'   (a data.frame summary of every task's outcome).
#' @export
run_dag <- function(dag, execution_date = Sys.time(), executor = c("sequential", "future"),
                     con = NULL, run_id = NULL, conf = list(), verbose = TRUE, run_type = "manual") {
  executor <- match.arg(executor)
  dag$validate()
  topo <- dag$topo()
  run_id <- run_id %||% paste0(dag$dag_id, "__", format(as.POSIXct(execution_date), "%Y%m%dT%H%M%S"),
                                "__", substr(uuid::UUIDgenerate(), 1, 8))

  if (!is.null(con)) db_insert_dag_run(con, run_id, dag$dag_id, execution_date, state = "running",
                                        run_type = run_type, conf = conf)

  states <- setNames(rep("pending", length(topo$order)), topo$order)
  results <- list()
  summary_rows <- list()

  if (verbose) cat(sprintf("=== rflow: DAG '%s' run '%s' (%s executor) ===\n", dag$dag_id, run_id, executor))

  run_one <- function(task_id) {
    task <- dag$get_task(task_id)
    up_states <- if (length(task$upstream_task_ids) == 0) character(0) else states[task$upstream_task_ids]
    if (!.eval_trigger_rule(task$trigger_rule, up_states)) {
      return(list(task_id = task_id, state = "skipped", result = NULL, error = NA_character_,
                  try_number = 0, max_tries = task$retries + 1,
                  start_date = Sys.time(), end_date = Sys.time(), log = "Skipped: trigger_rule not satisfied"))
    }
    ctx <- rflow_macro_context(execution_date, dag_id = dag$dag_id, task_id = task_id, run_id = run_id,
                                extra = c(list(ti = results), conf))
    out <- .run_single_task(task, ctx, results)
    out$task_id <- task_id
    if (identical(out$state, "success") && is.function(task$on_success_callback)) {
      .run_callback(task$on_success_callback, ctx, out$result)
    } else if (identical(out$state, "failed") && is.function(task$on_failure_callback)) {
      .run_callback(task$on_failure_callback, ctx, out$error)
    }
    out
  }

  for (layer in topo$layers) {
    if (executor == "sequential" || length(layer) == 1) {
      layer_out <- lapply(layer, run_one)
    } else {
      layer_out <- future.apply::future_lapply(layer, run_one, future.seed = TRUE)
    }
    for (out in layer_out) {
      states[out$task_id] <- out$state
      results[[out$task_id]] <- out$result
      summary_rows[[out$task_id]] <- data.frame(
        task_id = out$task_id, state = out$state,
        try_number = out$try_number, max_tries = out$max_tries,
        duration = as.numeric(difftime(out$end_date, out$start_date, units = "secs")),
        error = out$error %||% NA_character_, stringsAsFactors = FALSE
      )
      if (!is.null(con)) {
        db_upsert_task_instance(con, run_id, dag$dag_id, out$task_id, execution_date, out$state,
                                 try_number = out$try_number, max_tries = out$max_tries,
                                 start_date = out$start_date, end_date = out$end_date,
                                 duration = as.numeric(difftime(out$end_date, out$start_date, units = "secs")),
                                 xcom = out$result, error = out$error, log = out$log)
      }
      if (verbose) {
        icon <- switch(out$state, success = "OK", failed = "FAIL", skipped = "SKIP", "?")
        cat(sprintf("  [%s] %-30s %s\n", icon, out$task_id,
                    if (!is.na(out$error %||% NA)) paste0("- ", out$error) else ""))
      }
    }
  }

  dag_state <- if (any(states == "failed")) "failed" else "success"
  if (!is.null(con)) db_update_dag_run_state(con, run_id, dag_state)
  if (verbose) cat(sprintf("=== DAG run '%s' finished: %s ===\n", run_id, toupper(dag_state)))

  invisible(list(
    run_id = run_id,
    dag_id = dag$dag_id,
    state = dag_state,
    results = results,
    task_instances = do.call(rbind, summary_rows)
  ))
}

#' Backfill a DAG over a range of execution dates
#'
#' Analogous to `airflow dags backfill`. Triggers one run per scheduled
#' interval between `start_date` and `end_date`.
#'
#' @inheritParams run_dag
#' @param start_date first execution date (inclusive)
#' @param end_date last execution date (inclusive)
#' @return An invisible list of per-run results as returned by [run_dag()].
#' @export
backfill <- function(dag, start_date, end_date = Sys.time(), executor = "sequential",
                      con = NULL, verbose = TRUE) {
  step <- rflow_schedule_to_seconds(dag$schedule_interval)
  if (is.na(step)) stop("rflow: DAG '", dag$dag_id, "' has no schedule_interval to backfill against.")
  dates <- seq(as.POSIXct(start_date), as.POSIXct(end_date), by = step)
  runs <- lapply(dates, function(ed) run_dag(dag, execution_date = ed, executor = executor, con = con, verbose = verbose))
  invisible(runs)
}
