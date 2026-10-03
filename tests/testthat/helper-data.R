# Shared tiny tables. Cluster / Coiled tests should stay this small.

dasklyr_students <- function(na_name = FALSE) {
  students <- data.frame(
    name = c("Ann", "Bob", "Cara", "Dan"),
    class = c("a", "a", "b", "b"),
    math = c(92, 78, 88, 95),
    english = c(85, 90, 76, 88),
    stringsAsFactors = FALSE
  )
  if (isTRUE(na_name)) {
    students <- rbind(
      students,
      data.frame(
        name = NA_character_,
        class = "a",
        math = 70,
        english = 80,
        stringsAsFactors = FALSE
      )
    )
  }
  students
}

dasklyr_classes <- function() {
  data.frame(
    class = c("a", "b"),
    teacher = c("Chen", "Liu"),
    stringsAsFactors = FALSE
  )
}

dasklyr_coiled_fixture_clusters <- function() {
  tibble::tibble(
    id = c(2150039L, 2150050L, 935418L),
    name = c("demo-keep", "demo-keep", "other"),
    state = c("stopped", "ready", "stopped")
  )
}
