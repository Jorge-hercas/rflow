test_that("rflow_db_connect() accepts an already-open DBIConnection", {
  raw_con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  con <- rflow_db_connect(raw_con)
  expect_identical(con, raw_con)
  expect_true(DBI::dbExistsTable(con, "dag_run"))
  expect_true(DBI::dbExistsTable(con, "task_instance"))
  DBI::dbDisconnect(con)
})

test_that("db_insert_dag_run() upserts via ON CONFLICT instead of INSERT OR REPLACE", {
  con <- rflow_db_connect(":memory:")
  db_insert_dag_run(con, "run1", "dagA", "2026-01-01", state = "running")
  db_insert_dag_run(con, "run1", "dagA", "2026-01-01", state = "success")
  runs <- rflow_db_runs(con, "dagA")
  expect_equal(nrow(runs), 1)
  expect_equal(runs$state, "success")
  DBI::dbDisconnect(con)
})
