test_that("catchup = TRUE returns every missed interval since start_date", {
  con <- rflow_db_connect(":memory:")
  dag <- with_dag(DAG$new("catchup_yes", schedule_interval = "1 sec",
                           start_date = Sys.time() - 5, catchup = TRUE), {
    r_task("t", function() 1)
  })
  due <- rflow:::.rflow_due_execution_dates(dag, con = con)
  expect_gte(length(due), 4)
  DBI::dbDisconnect(con)
})

test_that("catchup = FALSE (default) only returns the single latest due interval", {
  con <- rflow_db_connect(":memory:")
  dag <- with_dag(DAG$new("catchup_no", schedule_interval = "1 sec",
                           start_date = Sys.time() - 5, catchup = FALSE), {
    r_task("t", function() 1)
  })
  due <- rflow:::.rflow_due_execution_dates(dag, con = con)
  expect_equal(length(due), 1)
  DBI::dbDisconnect(con)
})

test_that("nothing is due before the first interval has elapsed", {
  con <- rflow_db_connect(":memory:")
  dag <- with_dag(DAG$new("catchup_future", schedule_interval = "1 hour",
                           start_date = Sys.time(), catchup = TRUE), {
    r_task("t", function() 1)
  })
  due <- rflow:::.rflow_due_execution_dates(dag, con = con)
  expect_length(due, 0)
  DBI::dbDisconnect(con)
})

test_that("scheduler_run() keeps polling after an unexpected (non-task) error", {
  bad_con <- structure(list(), class = "not_a_real_connection")
  dag <- with_dag(DAG$new("sched_resilience", schedule_interval = "1 sec",
                           start_date = Sys.time() - 10), {
    r_task("t", function() 1)
  })
  # Should complete both ticks without raising, despite `bad_con` breaking
  # every DB call `.rflow_due_execution_dates()` makes.
  expect_error(
    scheduler_run(list(dag), con = bad_con, poll_interval = 0, max_ticks = 2),
    NA
  )
})

test_that("a DAG with schedule_interval = '@once' is never due", {
  con <- rflow_db_connect(":memory:")
  dag <- with_dag(DAG$new("manual_only", schedule_interval = "@once"), {
    r_task("t", function() 1)
  })
  due <- rflow:::.rflow_due_execution_dates(dag, con = con)
  expect_length(due, 0)
  DBI::dbDisconnect(con)
})
