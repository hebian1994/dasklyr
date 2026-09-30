test_that("ddf_* helpers and dask_invoke match sparklyr sdf_/invoke role", {
  skip_if_no_dask()
  library(dplyr)
  sc <- dask_connect(master = "local")
  on.exit(dask_disconnect(sc), add = TRUE)

  students <- data.frame(
    name = c("Ann", "Bob", "Cara", "Dan"),
    class = c("a", "a", "b", "b"),
    math = c(92, 78, 88, 95),
    english = c(85, 90, 76, 88),
    stringsAsFactors = FALSE
  )
  tbl <- copy_to(sc, students, overwrite = TRUE, repartition = 2L)

  expect_equal(ddf_npartitions(tbl), 2L)
  expect_equal(ddf_ncol(tbl), 4L)
  expect_equal(ddf_nrow(tbl), 4L)
  expect_equal(ddf_dim(tbl), c(4L, 4L))
  expect_equal(ddf_columns(tbl), c("name", "class", "math", "english"))

  schema <- ddf_schema(tbl)
  expect_equal(schema$name, c("name", "class", "math", "english"))
  expect_true(all(nzchar(schema$type)))

  tbl1 <- ddf_repartition(tbl, npartitions = 1L)
  expect_equal(ddf_npartitions(tbl1), 1L)
  expect_equal(ddf_nrow(tbl1), 4L)

  persisted <- ddf_persist(tbl1)
  expect_s3_class(persisted, "tbl_dask")

  invoked <- dask_invoke(tbl1, "repartition", npartitions = 3L)
  expect_equal(ddf_npartitions(invoked), 3L)
  expect_true(reticulate::py_has_attr(dask_dataframe(invoked), "npartitions"))

  info <- dask_invoke(sc, "scheduler_info")
  expect_true(is.list(info) || inherits(info, "python.builtin.object"))
})
