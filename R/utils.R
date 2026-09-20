#' @keywords internal
`%||%` <- function(a, b) if (is.null(a)) b else a

#' Render {{ macro }} style templates against a context list
#'
#' Supports Airflow-like macros: {{ ds }}, {{ ds_nodash }}, {{ execution_date }},
#' {{ dag_id }}, {{ task_id }}, {{ run_id }}, plus any custom key in `context`.
#'
#' @param text A character string possibly containing `{{ key }}` placeholders.
#' @param context A named list of values available for substitution.
#' @return The rendered character string.
#' @export
rflow_render <- function(text, context = list()) {
  if (is.null(text) || !is.character(text)) return(text)
  m <- gregexpr("\\{\\{\\s*([a-zA-Z0-9_\\.]+)\\s*\\}\\}", text)
  matches <- regmatches(text, m)[[1]]
  if (length(matches) == 0) return(text)
  out <- text
  for (mm in unique(matches)) {
    key <- gsub("\\{\\{\\s*|\\s*\\}\\}", "", mm)
    val <- context[[key]]
    if (is.null(val)) next
    out <- gsub(mm, as.character(val), out, fixed = TRUE)
  }
  out
}

#' Recursively render {{ macro }} templates through a nested list of op_args
#'
#' Walks a (possibly nested) list of task `op_args` and applies
#' [rflow_render()] to every character element, leaving non-character
#' elements untouched. This is what makes `{{ ds }}`-style macros actually
#' get substituted into task arguments before the task function runs.
#'
#' @param args A named list (typically a `Task`'s `op_args`).
#' @param context A named list as produced by [rflow_macro_context()].
#' @return The same structure as `args`, with character values rendered.
#' @keywords internal
rflow_render_args <- function(args, context = list()) {
  if (is.character(args)) {
    return(vapply(args, rflow_render, character(1), context = context, USE.NAMES = FALSE))
  }
  if (is.list(args)) {
    return(lapply(args, rflow_render_args, context = context))
  }
  args
}

#' Build the templating context for a given execution date
#'
#' Produces the named list (`ds`, `ds_nodash`, `ts`, `execution_date`, `dag_id`,
#' `task_id`, `run_id`, plus anything in `extra` such as `ti`) that is passed
#' as `context` into every task function, and used by [rflow_render()].
#'
#' @param execution_date A Date/POSIXct/parseable string.
#' @param dag_id,task_id,run_id Identifiers to embed in the context.
#' @param extra A named list merged in on top (e.g. `list(ti = results)`).
#' @return A named list.
#' @export
rflow_macro_context <- function(execution_date, dag_id = NA, task_id = NA, run_id = NA, extra = list()) {
  ed <- as.POSIXct(execution_date, tz = "UTC")
  ctx <- list(
    ds = format(ed, "%Y-%m-%d"),
    ds_nodash = format(ed, "%Y%m%d"),
    ts = format(ed, "%Y-%m-%dT%H:%M:%S"),
    execution_date = format(ed, "%Y-%m-%d %H:%M:%S"),
    dag_id = dag_id,
    task_id = task_id,
    run_id = run_id %||% NA
  )
  modifyList(ctx, extra)
}

#' Topologically sort a set of tasks given an adjacency list of dependencies
#'
#' @param task_ids character vector of all task ids
#' @param upstream_map named list: task_id -> character vector of upstream task_ids
#' @return list(order = character vector in valid execution order, layers = list of character vectors,
#'   where layers[[i]] are tasks that can run in parallel at "depth" i)
#' @keywords internal
rflow_topo_sort <- function(task_ids, upstream_map) {
  indeg <- setNames(integer(length(task_ids)), task_ids)
  for (tid in task_ids) {
    ups <- upstream_map[[tid]] %||% character(0)
    indeg[[tid]] <- length(ups)
  }
  remaining <- task_ids
  order <- character(0)
  layers <- list()
  depth_of <- setNames(integer(length(task_ids)), task_ids)

  # Kahn's algorithm, tracked in layers for parallel execution planning
  resolved <- character(0)
  iter_guard <- 0
  while (length(remaining) > 0) {
    iter_guard <- iter_guard + 1
    if (iter_guard > length(task_ids) + 5) {
      stop("rflow: cycle detected in DAG (unable to topologically sort). ",
           "Remaining tasks: ", paste(remaining, collapse = ", "))
    }
    ready <- remaining[vapply(remaining, function(tid) {
      ups <- upstream_map[[tid]] %||% character(0)
      all(ups %in% resolved)
    }, logical(1))]
    if (length(ready) == 0) {
      stop("rflow: cycle detected in DAG involving tasks: ", paste(remaining, collapse = ", "))
    }
    for (tid in ready) {
      ups <- upstream_map[[tid]] %||% character(0)
      depth_of[[tid]] <- if (length(ups) == 0) 1L else max(depth_of[ups]) + 1L
    }
    layers[[length(layers) + 1]] <- ready
    order <- c(order, ready)
    resolved <- c(resolved, ready)
    remaining <- setdiff(remaining, ready)
  }
  list(order = order, layers = layers, depth = depth_of)
}

#' Detect cycles explicitly (used for early validation with a friendlier message)
#' @keywords internal
rflow_check_acyclic <- function(task_ids, upstream_map) {
  invisible(rflow_topo_sort(task_ids, upstream_map))
}

#' Format duration in a human friendly way
#' @keywords internal
rflow_fmt_duration <- function(secs) {
  if (is.na(secs)) return(NA_character_)
  if (secs < 60) return(sprintf("%.2fs", secs))
  if (secs < 3600) return(sprintf("%dm %ds", as.integer(secs %/% 60), as.integer(secs %% 60)))
  sprintf("%dh %dm", as.integer(secs %/% 3600), as.integer((secs %% 3600) %/% 60))
}

#' Parse a schedule_interval spec into seconds. Supports cron-lite presets and
#' "N unit" strings (e.g. "5 mins", "1 hour", "1 day"), or NULL for manual-only.
#' @keywords internal
rflow_schedule_to_seconds <- function(schedule_interval) {
  if (is.null(schedule_interval) || identical(schedule_interval, "@once") || identical(schedule_interval, NA)) {
    return(NA_real_)
  }
  presets <- c("@hourly" = 3600, "@daily" = 86400, "@weekly" = 604800,
               "@monthly" = 2592000, "@yearly" = 31536000)
  if (schedule_interval %in% names(presets)) return(unname(presets[schedule_interval]))
  mm <- regmatches(schedule_interval, regexec("^\\s*([0-9]+)\\s*(sec|second|min|minute|hour|day|week)s?\\s*$",
                                               schedule_interval, ignore.case = TRUE))[[1]]
  if (length(mm) == 3) {
    n <- as.numeric(mm[2])
    unit <- tolower(mm[3])
    mult <- switch(unit, sec = 1, second = 1, min = 60, minute = 60,
                   hour = 3600, day = 86400, week = 604800)
    return(n * mult)
  }
  stop("rflow: could not parse schedule_interval '", schedule_interval,
       "'. Use one of @hourly/@daily/@weekly/@monthly/@yearly/@once, or 'N unit' e.g. '5 mins'.")
}
