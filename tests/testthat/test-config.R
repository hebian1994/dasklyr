test_that("dask_config returns overrides", {
  cfg <- dask_config(timeout = 5L, processes = TRUE)
  expect_type(cfg, "list")
  expect_equal(cfg$timeout, 5L)
  expect_true(cfg$processes)
})

test_that("connection helpers work without Dask", {
  expect_false(dask_connection_is_open(NULL))
  expect_false(dask_connection_is_open(list()))
  expect_equal(dask_connection_find(), list())
  expect_equal(dask_disconnect_all(), 0)
})

test_that("dask_available returns a single logical", {
  skip_on_cran()
  got <- dask_available()
  expect_type(got, "logical")
  expect_length(got, 1L)
  expect_false(is.na(got))
})
