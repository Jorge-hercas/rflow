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

    #' @description Create a new Task. Normally called via [r_task()].
    initialize = function(task_id, func, op_args = list(), retries = 0,
                           retry_delay = 5, timeout = NULL,
                           trigger_rule = c("all_success", "all_done", "one_success", "one_failed", "none_failed"),
                           doc = NULL) {
      stopifnot(is.character(task_id), length(task_id) == 1, nzchar(task_id))
      if (!grepl("^[A-Za-z0-9_\\.\\-]+$", task_id)) {
        stop("rflow: invalid task_id '", task_id, "'. Use alphanumerics, '_', '-', '.' only.")
      }
      if (!is.function(func)) stop("rflow: `func` must be an R function for task '", task_id, "'")
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
#' @param timeout Optional per-task timeout in seconds.
#' @param trigger_rule One of "all_success" (default), "all_done", "one_success",
#'   "one_failed", "none_failed".
#' @param doc Optional free-text documentation for the task.
#' @param dag The DAG to attach to. Defaults to the currently active DAG set by
#'   `with_dag()`.
#' @return The created [Task] object (invisibly usable with `%>>%` chaining).
#' @export
r_task <- function(task_id, func, op_args = list(), retries = 0, retry_delay = 5,
                    timeout = NULL, trigger_rule = "all_success", doc = NULL,
                    dag = rflow_active_dag()) {
  if (is.null(dag)) {
    stop("rflow: no active DAG. Wrap task definitions in with_dag(dag, { ... }) ",
         "or pass dag = my_dag explicitly.")
  }
  t <- Task$new(task_id = task_id, func = func, op_args = op_args, retries = retries,
                retry_delay = retry_delay, timeout = timeout, trigger_rule = trigger_rule, doc = doc)
  dag$add_task(t)
  t
}
