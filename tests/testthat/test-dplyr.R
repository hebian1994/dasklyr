test_that("dplyr verbs: mutate constants, filter in/na, summarise, joins", {
  skip_if_no_dask()
  library(dplyr)
  sc <- dask_connect(master = "local")
  on.exit(dask_disconnect(sc), add = TRUE)

  students <- dasklyr_students(na_name = TRUE)
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

test_that("summarise n() with other aggs after a join (two group keys)", {
  skip_if_no_dask()
  library(dplyr)
  sc <- dask_connect(master = "local")
  on.exit(dask_disconnect(sc), add = TRUE)

  set.seed(1)
  orders <- data.frame(
    customer_id = sample.int(8L, 80L, replace = TRUE),
    product_id = sample.int(6L, 80L, replace = TRUE),
    qty = sample.int(5L, 80L, replace = TRUE),
    amount = runif(80L, 5, 40)
  )
  products <- data.frame(
    product_id = 1:6,
    category = rep(c("A", "B", "C"), each = 2L),
    price = c(10, 12, 20, 22, 30, 32),
    stringsAsFactors = FALSE
  )
  customers <- data.frame(
    customer_id = 1:8,
    region = rep(c("R1", "R2"), each = 4L),
    stringsAsFactors = FALSE
  )
  od <- copy_to(sc, orders, overwrite = TRUE, repartition = 4L)
  pd <- copy_to(sc, products, overwrite = TRUE, repartition = 2L)
  cd <- copy_to(sc, customers, overwrite = TRUE, repartition = 2L)

  out <- od %>%
    inner_join(pd, by = "product_id") %>%
    mutate(revenue = qty * price) %>%
    left_join(cd, by = "customer_id") %>%
    group_by(region, category) %>%
    summarise(
      n = n(),
      revenue = sum(revenue),
      avg_qty = mean(qty),
      max_qty = max(qty),
      n_cust = n_distinct(customer_id),
      .groups = "drop"
    ) %>%
    collect() %>%
    arrange(region, category)

  expect_true(nrow(out) >= 1L)
  expect_equal(
    sort(names(out)),
    sort(c("region", "category", "n", "revenue", "avg_qty", "max_qty", "n_cust"))
  )
  expect_true(all(out$n >= 1L))
  expect_true(all(out$max_qty >= out$avg_qty))
})

