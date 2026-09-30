test_that("local dplyr verbs and parquet roundtrip", {
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

  scored <- tbl %>%
    dplyr::mutate(total = math + english) %>%
    dplyr::arrange(dplyr::desc(total)) %>%
    head(1)
  expect_equal(scored$name, "Dan")

  classes <- data.frame(
    class = c("a", "b"),
    teacher = c("Chen", "Liu"),
    stringsAsFactors = FALSE
  )
  joined <- tbl %>%
    dplyr::left_join(copy_to(sc, classes, overwrite = TRUE), by = "class") %>%
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
