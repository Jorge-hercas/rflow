#' Task: a single unit of work inside a DAG
#'
#' A `Task` wraps a plain R function (the "operator body") together with
#' scheduling metadata: retries, retry delay, a trigger rule, and its
#' upstream/downstream dependencies. Tasks are created with [r_task()] and
#' attached to a [DAG] object; they are rarely constructed directly.
#'
#' @export
Task <- R6::R6Class("Task",
  public = list(
    task_id = NULL,
    func = NULL,
    op_args = NULL,
    retries = 0,
    retry_delay = 5,
    timeout = NULL,
    trigger_rule = "all_success",
    dag = NULL,
    upstream_task_ids = NULL,
    downstream_task_ids = NULL,
    doc = NULL,
    on_success_callback = NULL,
    on_failure_callback = NULL,
    .explicit_args = NULL,

    #' @description Create a new Task. Normally called via [r_task()].
    #' @param .explicit_args Internal: named logical vector recording which of
    #'   `retries`/`retry_delay`/`timeout`/`trigger_rule`/`on_success_callback`/
    #'   `on_failure_callback` the caller set explicitly, so that a DAG's
    #'   `default_args` only fill in the ones that were *not* explicitly set
    #'   (mirroring Airflow's `default_args` semantics, where per-task
    #'   arguments always win). Set automatically by [r_task()]; treated as
    #'   "nothing explicit" when a `Task` is constructed directly.
    initialize = function(task_id, func, op_args = list(), retries = 0,
                           retry_delay = 5, timeout = NULL,
                           trigger_rule = c("all_success", "all_done", "one_success", "one_failed", "none_failed"),
                           doc = NULL, on_success_callback = NULL, on_failure_callback = NULL,
                           .explicit_args = NULL) {
      stopifnot(is.character(task_id), length(task_id) == 1, nzchar(task_id))
      if (!grepl("^[A-Za-z0-9_\\.\\-]+$", task_id)) {
        stop("rflow: invalid task_id '", task_id, "'. Use alphanumerics, '_', '-', '.' only.")
      }
      if (!is.function(func)) stop("rflow: `func` must be an R function for task '", task_id, "'")
      if (!is.null(on_success_callback) && !is.function(on_success_callback)) {
        stop("rflow: `on_success_callback` must be a function for task '", task_id, "'")
      }
      if (!is.null(on_failure_callback) && !is.function(on_failure_callback)) {
        stop("rflow: `on_failure_callback` must be a function for task '", task_id, "'")
      }
      trigger_rule <- match.arg(trigger_rule)
      self$task_id <- task_id
      self$func <- func
      self$op_args <- op_args
      self$retries <- retries
      self$retry_delay <- retry_delay
      self$timeout <- timeout
      self$trigger_rule <- trigger_rule
      self$upstream_task_ids <- character(0)
      self$downstream_task_ids <- character(0)
      self$doc <- doc
      self$on_success_callback <- on_success_callback
      self$on_failure_callback <- on_failure_callback
      default_flags <- c(retries = FALSE, retry_delay = FALSE, timeout = FALSE,
                          trigger_rule = FALSE, on_success_callback = FALSE, on_failure_callback = FALSE)
      if (!is.null(.explicit_args)) default_flags[names(.explicit_args)] <- .explicit_args
      self$.explicit_args <- default_flags
    },

    #' @description Declare that `other` (a Task or list/vector of Tasks) must
    #'   run before this task.
    set_upstream = function(other) {
      others <- .as_task_list(other)
      for (o in others) {
        if (!(o$task_id %in% self$upstream_task_ids)) {
          self$upstream_task_ids <- c(self$upstream_task_ids, o$task_id)
        }
        if (!(self$task_id %in% o$downstream_task_ids)) {
          o$downstream_task_ids <- c(o$downstream_task_ids, self$task_id)
        }
      }
      invisible(self)
    },

    #' @description Declare that `other` (a Task or list/vector of Tasks) must
    #'   run after this task.
    set_downstream = function(other) {
      others <- .as_task_list(other)
      for (o in others) o$set_upstream(self)
      invisible(self)
    },

    #' @description Pretty-print a task
    print = function(...) {
      cat(sprintf("<Task '%s'> retries=%d trigger_rule=%s upstream=[%s] downstream=[%s]\n",
                  self$task_id, self$retries, self$trigger_rule,
                  paste(self$upstream_task_ids, collapse = ", "),
                  paste(self$downstream_task_ids, collapse = ", ")))
      invisible(self)
    }
  )
)

#' @keywords internal
.as_task_list <- function(x) {
  if (inherits(x, "Task")) return(list(x))
  if (is.list(x)) {
    if (!all(vapply(x, inherits, logical(1), what = "Task"))) {
      stop("rflow: expected Task object(s)")
    }
    return(x)
  }
  stop("rflow: expected a Task or list of Tasks")
}

#' Define a task (an operator instance) inside the currently active DAG
#'
#' This is the primary way to define work in rflow, analogous to instantiating
#' a `PythonOperator` in Apache Airflow. Must be called inside a `with_dag()`
#' block, or with an explicit `dag =` argument.
#'
#' @param task_id Unique identifier for the task within its DAG.
#' @param func An R function to execute. It may accept named arguments matching
#'   `op_args`, and may optionally declare a `context` parameter to receive
#'   scheduling metadata (execution_date, ds, dag_id, task_id, run_id, ti — a
#'   list of upstream return values keyed by task_id, i.e. R's answer to
#'   Airflow's XCom).
#' @param op_args A named list of arguments passed to `func`.
#' @param retries Number of retries on failure (default 0).
#' @param retry_delay Seconds to wait between retries (default 5).
#' @param timeout Optional per-task timeout in seconds. Enforced via
#'   [base::setTimeLimit()], which interrupts ordinary R-level computation
#'   (loops, vectorized operations, most function calls) once the elapsed
#'   time is exceeded. Like `setTimeLimit()` itself, it is checked only at R
#'   interrupt points, so it will **not** preempt a single long-running call
#'   with no interrupt points inside it -- notably `Sys.sleep()`, and most
#'   blocking system/network calls implemented in C without periodic
#'   interrupt checks. For tasks that call slow external services, prefer
#'   giving the underlying call its own timeout (e.g. an HTTP client's
#'   `timeout` option) rather than relying solely on this parameter. A
#'   timed-out attempt is treated like any other failure and is retried per
#'   `retries`.
#' @param trigger_rule One of "all_success" (default), "all_done", "one_success",
#'   "one_failed", "none_failed".
#' @param doc Optional free-text documentation for the task.
#' @param on_success_callback Optional function called as
#'   `f(context, result)` right after the task finishes successfully (any
#'   error it raises is logged as a warning and does not fail the task).
#' @param on_failure_callback Optional function called as
#'   `f(context, error)` after the task exhausts its retries and is marked
#'   `"failed"` (any error it raises is logged as a warning and does not
#'   change the task's outcome). Note: with `executor = "future"`, callbacks
#'   run inside the worker that ran the task, so with a non-local
#'   `future::plan()` they may execute on a different machine/process than
#'   the one that called [run_dag()].
#' @param dag The DAG to attach to. Defaults to the currently active DAG set by
#'   `with_dag()`.
#' @return The created [Task] object (invisibly usable with `%>>%` chaining).
#' @export
r_task <- function(task_id, func, op_args = list(), retries = 0, retry_delay = 5,
                    timeout = NULL, trigger_rule = "all_success", doc = NULL,
                    on_success_callback = NULL, on_failure_callback = NULL,
                    dag = rflow_active_dag()) {
  if (is.null(dag)) {
    stop("rflow: no active DAG. Wrap task definitions in with_dag(dag, { ... }) ",
         "or pass dag = my_dag explicitly.")
  }
  explicit <- c(
    retries = !missing(retries), retry_delay = !missing(retry_delay),
    timeout = !missing(timeout), trigger_rule = !missing(trigger_rule),
    on_success_callback = !missing(on_success_callback), on_failure_callback = !missing(on_failure_callback)
  )
  t <- Task$new(task_id = task_id, func = func, op_args = op_args, retries = retries,
                retry_delay = retry_delay, timeout = timeout, trigger_rule = trigger_rule, doc = doc,
                on_success_callback = on_success_callback, on_failure_callback = on_failure_callback,
                .explicit_args = explicit)
  dag$add_task(t)
  t
}
