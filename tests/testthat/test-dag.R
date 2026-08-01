test_that("tasks chain dependencies correctly with %>>%", {
  dag <- with_dag(DAG$new("t1"), {
    a <- r_task("a", function() 1)
    b <- r_task("b", function() 2)
    c <- r_task("c", function() 3)
    a %>>% b %>>% c
  })
  expect_equal(dag$get_task("b")$upstream_task_ids, "a")
  expect_equal(dag$get_task("c")$upstream_task_ids, "b")
  expect_equal(dag$get_task("a")$downstream_task_ids, "b")
})

test_that("fan-out / fan-in works with lists of tasks", {
  dag <- with_dag(DAG$new("t2"), {
    a <- r_task("a", function() 1)
    b1 <- r_task("b1", function() 2)
    b2 <- r_task("b2", function() 3)
    c <- r_task("c", function() 4)
    a %>>% list(b1, b2) %>>% c
  })
  expect_setequal(dag$get_task("c")$upstream_task_ids, c("b1", "b2"))
  expect_setequal(dag$get_task("a")$downstream_task_ids, c("b1", "b2"))
})

test_that("duplicate task_id errors", {
  expect_error({
    with_dag(DAG$new("t3"), {
      r_task("dup", function() 1)
      r_task("dup", function() 2)
    })
  }, "already exists")
})

test_that("cycles are detected", {
  dag <- with_dag(DAG$new("t4"), {
    a <- r_task("a", function() 1)
    b <- r_task("b", function() 2)
  })
  a <- dag$get_task("a"); b <- dag$get_task("b")
  a %>>% b
  b %>>% a
  expect_error(dag$validate(), "cycle")
})

test_that("topological sort respects dependencies", {
  dag <- with_dag(DAG$new("t5"), {
    a <- r_task("a", function() 1)
    b <- r_task("b", function() 2)
    c <- r_task("c", function() 3)
    a %>>% b
    b %>>% c
  })
  topo <- dag$topo()
  expect_equal(match("a", topo$order) < match("b", topo$order), TRUE)
  expect_equal(match("b", topo$order) < match("c", topo$order), TRUE)
})
