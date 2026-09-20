test_that("op_args are rendered with {{ ds }}-style macros before the task runs", {
  dag <- with_dag(DAG$new("templ_test", start_date = "2026-01-01"), {
    r_task("say_ds", function(msg) msg, op_args = list(msg = "today is {{ ds }}"))
  })
  res <- run_dag(dag, execution_date = "2026-03-15", verbose = FALSE)
  expect_equal(res$results$say_ds, "today is 2026-03-15")
})

test_that("op_args rendering works through nested lists too", {
  dag <- with_dag(DAG$new("templ_nested"), {
    r_task("echo", function(cfg) cfg, op_args = list(cfg = list(a = "run={{ run_id }}", b = 2)))
  })
  res <- run_dag(dag, run_id = "manualrun1", verbose = FALSE)
  expect_equal(res$results$echo$a, "run=manualrun1")
  expect_equal(res$results$echo$b, 2)
})

test_that("timeout fails a long-running (CPU-bound) task and it can be retried", {
  dag <- with_dag(DAG$new("timeout_test"), {
    r_task("slow", function() { x <- 0; repeat { x <- x + 1 } }, timeout = 1, retries = 0)
  })
  res <- run_dag(dag, verbose = FALSE)
  expect_equal(res$state, "failed")
  expect_match(res$task_instances$error[1], "time limit")
})

test_that("a task without a timeout is unaffected", {
  dag <- with_dag(DAG$new("no_timeout_test"), {
    r_task("fast", function() 1 + 1)
  })
  res <- run_dag(dag, verbose = FALSE)
  expect_equal(res$state, "success")
  expect_equal(res$results$fast, 2)
})

test_that("DAG default_args fill in unset task fields but never override explicit ones", {
  dag <- with_dag(DAG$new("defaults_test", default_args = list(retries = 2, retry_delay = 9)), {
    r_task("explicit", function() 1, retries = 7)
    r_task("implicit", function() 1)
  })
  expect_equal(dag$get_task("explicit")$retries, 7)
  expect_equal(dag$get_task("implicit")$retries, 2)
  expect_equal(dag$get_task("implicit")$retry_delay, 9)
})

test_that("on_success_callback receives the task's result and on_failure_callback the error", {
  log_env <- new.env()
  dag <- with_dag(DAG$new("callback_test"), {
    r_task("good", function() 42, on_success_callback = function(context, result) log_env$ok <- result)
    r_task("bad", function() stop("boom"), retries = 0,
           on_failure_callback = function(context, error) log_env$fail <- error)
  })
  run_dag(dag, verbose = FALSE)
  expect_equal(log_env$ok, 42)
  expect_match(log_env$fail, "boom")
})

test_that("a callback that errors is caught (as a warning) and does not fail the task", {
  dag <- with_dag(DAG$new("callback_error_test"), {
    r_task("good", function() 1, on_success_callback = function(context, result) stop("callback exploded"))
  })
  expect_warning(res <- run_dag(dag, verbose = FALSE), "callback exploded")
  expect_equal(res$state, "success")
})
