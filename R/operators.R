#' Declare a downstream dependency: `lhs` must run before `rhs`
#'
#' The R-native equivalent of Airflow's `task1 >> task2`. Works with single
#' Tasks or lists/vectors of Tasks (fan-out / fan-in), and is chainable and
#' left-to-right associative just like Airflow's bitshift operators, e.g.
#' `extract %>>% list(transform_a, transform_b) %>>% load`.
#'
#' @param lhs A [Task], or a list of Tasks.
#' @param rhs A [Task], or a list of Tasks.
#' @return `rhs`, invisibly, to allow further chaining.
#' @export
`%>>%` <- function(lhs, rhs) {
  lhs_list <- .as_task_list(lhs)
  rhs_list <- .as_task_list(rhs)
  for (r in rhs_list) r$set_upstream(lhs_list)
  invisible(if (length(rhs_list) == 1) rhs_list[[1]] else rhs_list)
}

#' Declare an upstream dependency: `lhs` must run after `rhs`
#'
#' The R-native equivalent of Airflow's `task2 << task1`.
#'
#' @param lhs A [Task], or a list of Tasks.
#' @param rhs A [Task], or a list of Tasks.
#' @return `rhs`, invisibly.
#' @export
`%<<%` <- function(lhs, rhs) {
  lhs_list <- .as_task_list(lhs)
  rhs_list <- .as_task_list(rhs)
  for (l in lhs_list) l$set_upstream(rhs_list)
  invisible(if (length(rhs_list) == 1) rhs_list[[1]] else rhs_list)
}
