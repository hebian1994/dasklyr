# Connect to a self-managed Dask scheduler (Docker by default).
#
#   docker compose up -d --wait --scale worker=2
#   Rscript examples/cluster_demo.R
#
# Do not write parquet/csv to a laptop tempfile: workers cannot see it.
#
# Optional env:
#   DASK_SCHEDULER   default tcp://127.0.0.1:8786
#   DASKLYR_PYTHON / RETICULATE_PYTHON

if (!file.exists("examples/common.R")) {
  stop("Run from the package root: Rscript examples/cluster_demo.R", call. = FALSE)
}
source("examples/common.R")

library(dasklyr)
library(dplyr)

master <- Sys.getenv("DASK_SCHEDULER", unset = "tcp://127.0.0.1:8786")
sc <- dask_connect(master = master)
on.exit(dask_disconnect(sc), add = TRUE)
print(sc)
cat("dashboard: ", dask_web(sc, browse = FALSE), "\n", sep = "")

tables <- dasklyr_example_copy(sc)
dasklyr_example_dplyr(tables$st, tables$cl)

cat("\n--- persist ---\n")
print(ddf_nrow(ddf_persist(tables$st)))
cat("cluster demo ok\n")
