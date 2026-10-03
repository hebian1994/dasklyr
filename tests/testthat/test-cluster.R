test_that("docker/cluster scheduler runs dplyr verbs", {
  scheduler <- skip_if_no_cluster()
  library(dplyr)
  sc <- dask_connect(master = scheduler)
  on.exit(dask_disconnect(sc), add = TRUE)

  tbl <- copy_to(sc, dasklyr_students(), overwrite = TRUE, repartition = 2L)

  ping <- tbl %>%
    dplyr::filter(math >= 90) %>%
    dplyr::collect()
  expect_equal(sort(ping$name), c("Ann", "Dan"))

  by_class <- tbl %>%
    dplyr::group_by(class) %>%
    dplyr::summarise(n = n(), avg_math = mean(math), .groups = "drop") %>%
    dplyr::collect() %>%
    dplyr::arrange(class)
  expect_equal(by_class$n, c(2, 2))
  expect_equal(by_class$avg_math, c(85, 91.5))
})
