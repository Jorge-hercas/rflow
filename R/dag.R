.rflow_env <- new.env(parent = emptyenv())
.rflow_env$active_dag_stack <- list()
.rflow_env$registry <- new.env(parent = emptyenv())  # dag_id -> DAG

#' @keywords internal
rflow_active_dag <- function() {
  n <- length(.rflow_env$active_dag_stack)
  if (n == 0) return(NULL)
  .rflow_env$active_dag_stack[[n]]
}

#' DAG: a Directed Acyclic Graph of Tasks
#'
#' The R-native equivalent of Airflow's `DAG` class. Holds tasks, their
#' dependency graph, scheduling metadata (`schedule_interval`, `start_date`),
#' and default arguments applied to every task.
#'
#' @export
DAG <- R6::R6Class("DAG",
  public = list(
    dag_id = NULL,
    description = NULL,
    schedule_interval = NULL,
    start_date = NULL,
    catchup = FALSE,
    default_args = NULL,
    tasks = NULL,
    tags = NULL,

    #' @description Create a new DAG.
    #' @param dag_id Unique DAG identifier.
    #' @param description Free-text description.
    #' @param schedule_interval One of "@once" (default, manual-only),
    #'   "@hourly", "@daily", "@weekly", "@monthly", "@yearly", or a string like
    #'   "5 mins" / "2 hours".
    #' @param start_date A Date/POSIXct for the earliest scheduled run.
    #' @param catchup If TRUE, backfill() will materialize every missed
    #'   interval between start_date and now; if FALSE only the latest.
    #' @param default_args A named list merged into every task's arguments
    #'   (e.g. list(retries = 2, retry_delay = 10)).
    #' @param tags Optional character vector of tags, purely cosmetic.
    initialize = function(dag_id, description = NULL, schedule_interval = "@once",
                           start_date = Sys.Date(), catchup = FALSE,
                           default_args = list(), tags = character(0)) {
      stopifnot(is.character(dag_id), length(dag_id) == 1, nzchar(dag_id))
      self$dag_id <- dag_id
      self$description <- description
      self$schedule_interval <- schedule_interval
      self$start_date <- start_date
      self$catchup <- catchup
      self$default_args <- default_args
      self$tags <- tags
      self$tasks <- new.env(parent = emptyenv())
      assign(dag_id, self, envir = .rflow_env$registry)
    },

    #' @description Register a Task under this DAG (called by [r_task()]).
    add_task = function(task) {
      if (exists(task$task_id, envir = self$tasks, inherits = FALSE)) {
        stop("rflow: task_id '", task$task_id, "' already exists in DAG '", self$dag_id, "'")
      }
      task$dag <- self
      if (length(self$default_args) > 0) {
        for (nm in names(self$default_args)) {
          if (nm %in% c("retries", "retry_delay", "timeout", "trigger_rule")) {
            task[[nm]] <- self$default_args[[nm]]
          }
        }
      }
      assign(task$task_id, task, envir = self$tasks)
      invisible(self)
    },

    #' @description Get a Task by id.
    get_task = function(task_id) {
      if (!exists(task_id, envir = self$tasks, inherits = FALSE)) {
        stop("rflow: no such task '", task_id, "' in DAG '", self$dag_id, "'")
      }
      get(task_id, envir = self$tasks, inherits = FALSE)
    },

    #' @description List all task ids.
    task_ids = function() sort(ls(envir = self$tasks)),

    #' @description Return the named list of upstream task ids per task.
    upstream_map = function() {
      ids <- self$task_ids()
      setNames(lapply(ids, function(id) self$get_task(id)$upstream_task_ids), ids)
    },

    #' @description Validate the DAG: unique ids (guaranteed), no cycles, no
    #'   dangling dependency references.
    validate = function() {
      ids <- self$task_ids()
      um <- self$upstream_map()
      for (id in ids) {
        bad <- setdiff(um[[id]], ids)
        if (length(bad) > 0) {
          stop("rflow: task '", id, "' depends on unknown task(s): ", paste(bad, collapse = ", "))
        }
      }
      rflow_check_acyclic(ids, um)
      invisible(TRUE)
    },

    #' @description Compute topological execution order/layers.
    topo = function() {
      self$validate()
      rflow_topo_sort(self$task_ids(), self$upstream_map())
    },

    #' @description Pretty-print the DAG structure.
    print = function(...) {
      cat(sprintf("<DAG '%s'> schedule=%s start_date=%s tasks=%d\n",
                  self$dag_id, self$schedule_interval %||% "manual",
                  as.character(self$start_date), length(self$task_ids())))
      for (id in self$task_ids()) {
        t <- self$get_task(id)
        cat(sprintf("  - %s  (upstream: %s)\n", id,
                    if (length(t$upstream_task_ids)) paste(t$upstream_task_ids, collapse = ", ") else "-"))
      }
      invisible(self)
    }
  )
)

#' Create a DAG and make it the "active" DAG for the duration of `code`
#'
#' Mirrors Airflow's `with DAG(...) as dag:` context manager. Any [r_task()]
#' calls inside `code` that don't specify `dag=` explicitly attach to this DAG.
#'
#' @param dag Either a [DAG] object, or arguments are passed to `DAG$new()` if
#'   a dag_id string is given as the first argument via `...`.
#' @param code An expression (typically a `{ ... }` block) to evaluate with the
#'   DAG active.
#' @return The [DAG] object, invisibly.
#' @examples
#' \dontrun{
#' dag <- with_dag(DAG$new("my_dag", schedule_interval = "@daily"), {
#'   t1 <- r_task("extract", function() 1)
#'   t2 <- r_task("load", function(context) context$ti$extract + 1)
#'   t1 %>>% t2
#' })
#' }
#' @export
with_dag <- function(dag, code) {
  if (!inherits(dag, "DAG")) stop("rflow: with_dag() requires a DAG object as its first argument")
  .rflow_env$active_dag_stack[[length(.rflow_env$active_dag_stack) + 1]] <- dag
  on.exit({
    .rflow_env$active_dag_stack[[length(.rflow_env$active_dag_stack)]] <- NULL
  }, add = TRUE)
  force(code)
  invisible(dag)
}

#' Look up a previously created DAG by id from the in-session registry
#' @param dag_id character
#' @return a [DAG] object
#' @export
get_dag <- function(dag_id) {
  if (!exists(dag_id, envir = .rflow_env$registry, inherits = FALSE)) {
    stop("rflow: no DAG registered with id '", dag_id, "'")
  }
  get(dag_id, envir = .rflow_env$registry, inherits = FALSE)
}

#' List all DAG ids registered in the current R session
#' @export
list_dags <- function() {
  sort(ls(envir = .rflow_env$registry))
}
