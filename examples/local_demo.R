# Local Dask laptop demo (no cluster required).
#
#   pip install "dask[dataframe,distributed]" pyarrow pandas
#   Rscript examples/local_demo.R
#
# Optional env: DASKLYR_PYTHON or RETICULATE_PYTHON = path to that Python.

library(dasklyr)
library(dplyr)

sc <- dask_connect(master = "local")
on.exit(dask_disconnect(sc), add = TRUE)
print(sc)

students <- data.frame(
  name = c("Ann", "Bob", "Cara", "Dan"),
  class = c("a", "a", "b", "b"),
  math = c(92L, 78L, 88L, 95L),
  english = c(85L, 90L, 76L, 88L)
)
classes <- data.frame(
  class = c("a", "b"),
  teacher = c("Chen", "Liu")
)

st <- copy_to(sc, students, overwrite = TRUE, repartition = 2L)
cl <- copy_to(sc, classes, overwrite = TRUE, repartition = 1L)

cat("\n--- filter math >= 90 ---\n")
print(st %>% filter(math >= 90) %>% collect())

cat("\n--- mutate constant + filter %in% ---\n")
print(
  st %>%
    mutate(bonus = 5, total = math + english + bonus) %>%
    filter(name %in% c("Ann", "Dan")) %>%
    arrange(desc(total)) %>%
    collect()
)

cat("\n--- count / summarise without group_by ---\n")
print(st %>% count(class) %>% collect())
print(st %>% summarise(n = n(), avg_math = mean(math), .groups = "drop") %>% collect())

cat("\n--- group_by ---\n")
print(
  st %>%
    group_by(class) %>%
    summarise(n = n(), avg_math = mean(math), .groups = "drop") %>%
    collect()
)

cat("\n--- left_join ---\n")
print(
  st %>%
    left_join(cl, by = "class") %>%
    select(name, class, teacher, math) %>%
    collect()
)

pq <- tempfile("students_parquet")
dask_write_parquet(st, pq, overwrite = TRUE)
cat("\n--- parquet roundtrip ---\n")
print(dask_read_parquet(sc, "students_pq", pq) %>% collect())
cat("demo ok\n")
