test_that("dask_connect reuses master + app_name", {
  skip_if_no_dask()
  on.exit(dask_disconnect_all(), add = TRUE)
  sc1 <- dask_connect(master = "local", app_name = "reuse-test")
  expect_message(
    sc2 <- dask_connect(master = "local", app_name = "reuse-test"),
    "Re-using"
  )
  expect_true(identical(sc1, sc2))
  expect_true(dask_connection_is_open(sc1))
  expect_gt(nchar(dask_web(sc1, browse = FALSE)), 0)

  n <- dask_disconnect_all()
  expect_equal(n, 1)
  expect_false(dask_connection_is_open(sc1))

  sc3 <- dask_connect(master = "local", app_name = "reuse-test")
  expect_true(dask_connection_is_open(sc3))
  expect_false(identical(sc1, sc3))
})
