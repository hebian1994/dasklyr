test_that("dplyr verbs: mutate constants, filter in/na, summarise, joins", {
  skip_if_no_dask()
  library(dplyr)
  sc <- dask_connect(master = "local")
  on.exit(dask_disconnect(sc), add = TRUE)

  students <- data.frame(
    name = c("Ann", "Bob", "Cara", "Dan", NA_character_),
    class = c("a", "a", "b", "b", "a"),
    math = c(92, 78, 88, 95, 70),
    english = c(85, 90, 76, 88, 80),
    stringsAsFactors = FALSE
  )
  tbl <- copy_to(sc, students, overwrite = TRUE, repartition = 2L)
  classes <- copy_to(
    sc,
    data.frame(klass = c("a", "b"), teacher = c("Chen", "Liu"), stringsAsFactors = FALSE),
    "classes",
    overwrite = TRUE
  )

  bumped <- tbl %>%
    mutate(bonus = 5, total = math + english + bonus) %>%
    collect()
  expect_equal(bumped$total[bumped$name == "Ann" & !is.na(bumped$name)], 182)

  in_set <- tbl %>%
    filter(name %in% c("Ann", "Dan")) %>%
    collect()
  expect_equal(sort(in_set$name), c("Ann", "Dan"))

  missing <- tbl %>%
    filter(is.na(name)) %>%
    collect()
  expect_equal(nrow(missing), 1)

  present <- tbl %>%
    filter(!is.na(name)) %>%
    collect()
  expect_equal(nrow(present), 4)

  overall <- tbl %>%
    summarise(n = n(), avg_math = mean(math), .groups = "drop") %>%
    collect()
  expect_equal(overall$n, 5)
  expect_equal(overall$avg_math, mean(students$math))

  nd <- tbl %>%
    filter(!is.na(name)) %>%
    group_by(class) %>%
    summarise(n_names = n_distinct(name), .groups = "drop") %>%
    collect() %>%
    arrange(class)
  expect_equal(nd$n_names, c(2, 2))

  renamed <- tbl %>%
    rename(subject = class) %>%
    collect()
  expect_true("subject" %in% names(renamed))
  expect_false("class" %in% names(renamed))

  uniq <- tbl %>%
    distinct(class) %>%
    collect() %>%
    arrange(class)
  expect_equal(uniq$class, c("a", "b"))

  counted <- tbl %>%
    count(class) %>%
    collect() %>%
    arrange(class)
  expect_equal(counted$n, c(3, 2))

  top2 <- tbl %>%
    arrange(desc(math)) %>%
    slice_head(n = 2) %>%
    collect()
  expect_equal(nrow(top2), 2)
  expect_equal(top2$math[1], 95)

  innered <- tbl %>%
    filter(!is.na(name)) %>%
    inner_join(classes, by = c("class" = "klass")) %>%
    collect()
  expect_equal(sort(unique(innered$teacher)), c("Chen", "Liu"))

  semi <- tbl %>%
    filter(!is.na(name)) %>%
    semi_join(classes, by = c("class" = "klass")) %>%
    collect()
  expect_false("teacher" %in% names(semi))
  expect_equal(nrow(semi), 4)

  g <- tbl %>% group_by(class) %>% ungroup()
  expect_equal(g$groups, character())
})
