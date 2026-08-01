test_that("run_dag passes XCom-like results between tasks", {
  dag <- with_dag(DAG$new("xcom_test"), {
    up <- r_task("up", function() 21)
    down <- r_task("down", function(context) context$ti$up * 2)
    up %>>% down
  })
  res <- run_dag(dag, verbose = FALSE)
  expect_equal(res$state, "success")
  expect_equal(res$results$down, 42)
})

test_that("retries eventually succeed", {
  counter <- new.env(); counter$n <- 0
  dag <- with_dag(DAG$new("retry_test"), {
    r_task("flaky", function() {
      counter$n <- counter$n + 1
      if (counter$n < 2) stop("fail once")
      "ok"
    }, retries = 2, retry_delay = 0)
  })
  res <- run_dag(dag, verbose = FALSE)
  expect_equal(res$state, "success")
  expect_equal(counter$n, 2)
})

test_that("trigger_rule = all_done runs cleanup after upstream failure", {
  dag <- with_dag(DAG$new("cleanup_test"), {
    bad <- r_task("bad", function() stop("boom"))
    cleanup <- r_task("cleanup", function() "done", trigger_rule = "all_done")
    bad %>>% cleanup
  })
  res <- run_dag(dag, verbose = FALSE)
  expect_equal(res$state, "failed")
  ti <- res$task_instances
  expect_equal(ti[ti$task_id == "cleanup", "state"], "success")
})

test_that("default trigger_rule skips downstream of a failed upstream", {
  dag <- with_dag(DAG$new("skip_test"), {
    bad <- r_task("bad", function() stop("boom"))
    downstream <- r_task("downstream", function() "should be skipped")
    bad %>>% downstream
  })
  res <- run_dag(dag, verbose = FALSE)
  ti <- res$task_instances
  expect_equal(ti[ti$task_id == "downstream", "state"], "skipped")
})

test_that("SQLite persistence round-trips dag_run and task_instance rows", {
  con <- rflow_db_connect(":memory:")
  dag <- with_dag(DAG$new("db_test"), {
    r_task("only", function() "hi")
  })
  res <- run_dag(dag, con = con, verbose = FALSE)
  runs <- rflow_db_runs(con, "db_test")
  expect_equal(nrow(runs), 1)
  expect_equal(runs$state, "success")
  ti <- rflow_db_task_instances(con, res$run_id)
  expect_equal(ti$task_id, "only")
  expect_equal(ti$state, "success")
  DBI::dbDisconnect(con)
})
