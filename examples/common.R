# Shared tables / dplyr smoke for the three demos.
# Source this from the package root or from examples/.

dasklyr_example_students <- function() {
  data.frame(
    name = c("Ann", "Bob", "Cara", "Dan"),
    class = c("a", "a", "b", "b"),
    math = c(92L, 78L, 88L, 95L),
    english = c(85L, 90L, 76L, 88L)
  )
}

dasklyr_example_classes <- function() {
  data.frame(
    class = c("a", "b"),
    teacher = c("Chen", "Liu")
  )
}

dasklyr_example_copy <- function(sc) {
  st <- copy_to(
    sc,
    dasklyr_example_students(),
    "students",
    overwrite = TRUE,
    repartition = 2L
  )
  cl <- copy_to(
    sc,
    dasklyr_example_classes(),
    "classes",
    overwrite = TRUE,
    repartition = 1L
  )
  cat("src_tbls: ", paste(src_tbls(sc), collapse = ", "), "\n", sep = "")
  cat("npartitions(students): ", ddf_npartitions(st), "\n", sep = "")
  print(ddf_schema(st))
  list(st = st, cl = cl)
}

dasklyr_example_dplyr <- function(st, cl) {
  cat("\n--- filter math >= 90 ---\n")
  print(st %>% dplyr::filter(math >= 90) %>% dplyr::collect())

  cat("\n--- mutate + arrange ---\n")
  print(
    st %>%
      dplyr::mutate(total = math + english) %>%
      dplyr::arrange(dplyr::desc(total)) %>%
      dplyr::collect()
  )

  cat("\n--- group_by ---\n")
  print(
    st %>%
      dplyr::group_by(class) %>%
      dplyr::summarise(n = n(), avg_math = mean(math), .groups = "drop") %>%
      dplyr::collect()
  )

  cat("\n--- left_join ---\n")
  print(
    st %>%
      dplyr::left_join(cl, by = "class") %>%
      dplyr::select(name, class, teacher, math) %>%
      dplyr::collect()
  )
}
