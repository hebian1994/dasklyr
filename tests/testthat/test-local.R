test_that("local dplyr verbs and parquet roundtrip", {
  skip_if_no_dask()
  library(dplyr)
  sc <- dask_connect(master = "local")
  on.exit(dask_disconnect(sc), add = TRUE)

  students <- dasklyr_students()
  tbl <- copy_to(sc, students, overwrite = TRUE, repartition = 2L)

  ping <- tbl %>%
    dplyr::filter(math >= 90) %>%
    dplyr::collect()
  expect_equal(sort(ping$name), c("Ann", "Dan"))

  joined <- tbl %>%
    dplyr::left_join(copy_to(sc, dasklyr_classes(), overwrite = TRUE), by = "class") %>%
    dplyr::select(name, class, teacher, math) %>%
    dplyr::collect()
  expect_equal(sort(joined$teacher), c("Chen", "Chen", "Liu", "Liu"))

  tmp <- tempfile("dasklyr")
  dir.create(tmp)
  dask_write_parquet(tbl, tmp, overwrite = TRUE)
  back <- dask_read_parquet(sc, "roundtrip", tmp) %>% dplyr::collect()
  expect_equal(nrow(back), 4)

  expect_equal(tbl(sc, "students") %>% dplyr::collect() %>% nrow(), 4)
  expect_true("students" %in% src_tbls(sc))
  expect_true("roundtrip" %in% src_tbls(sc))
})
