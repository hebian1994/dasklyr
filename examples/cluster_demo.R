# Connect to a Dask scheduler (Docker by default).
#
#   docker compose up -d --wait --scale worker=2
#   Rscript examples/cluster_demo.R
#
# Optional env:
#   DASK_SCHEDULER   default tcp://127.0.0.1:8786
#   DASKLYR_PYTHON / RETICULATE_PYTHON

library(dasklyr)
library(dplyr)

master <- Sys.getenv("DASK_SCHEDULER", unset = "tcp://127.0.0.1:8786")
sc <- dask_connect(master = master)
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
cat("npartitions(st): ", ddf_npartitions(st), "\n", sep = "")
print(ddf_schema(st))

cat("\n--- filter math >= 90 ---\n")
print(st %>% filter(math >= 90) %>% collect())

cat("\n--- mutate + arrange ---\n")
print(
  st %>%
    mutate(total = math + english) %>%
    arrange(desc(total)) %>%
    collect()
)

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

cat("cluster demo ok\n")
