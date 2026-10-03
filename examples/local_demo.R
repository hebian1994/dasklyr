# Local Dask laptop demo (no cluster required).
#
#   pip install "dask[dataframe,distributed]" pyarrow pandas
#   Rscript examples/local_demo.R
#
# Optional env: DASKLYR_PYTHON or RETICULATE_PYTHON.

if (!file.exists("examples/common.R")) {
  stop("Run from the package root: Rscript examples/local_demo.R", call. = FALSE)
}
source("examples/common.R")

library(dasklyr)
library(dplyr)

sc <- dask_connect(master = "local")
on.exit(dask_disconnect(sc), add = TRUE)
print(sc)
invisible(dask_web(sc, browse = FALSE))

tables <- dasklyr_example_copy(sc)
st <- tables$st
cl <- tables$cl
dasklyr_example_dplyr(st, cl)

cat("\n--- ddf_* / invoke ---\n")
print(ddf_dim(st))
cat("repartition(3): ", ddf_npartitions(dask_invoke(st, "repartition", npartitions = 3L)), "\n", sep = "")

tmp <- tempfile("dasklyr_demo")
dir.create(tmp)
pq <- file.path(tmp, "students.parquet")
csv_dir <- file.path(tmp, "students_csv")
dask_write_parquet(st, pq, overwrite = TRUE)
dask_write_csv(st, csv_dir, index = FALSE)
cat("\n--- parquet / csv roundtrip ---\n")
print(dask_read_parquet(sc, "students_pq", pq) %>% collect())
print(dask_read_csv(sc, "students_csv", file.path(csv_dir, "*")) %>% collect())
cat("registered: ", paste(src_tbls(sc), collapse = ", "), "\n", sep = "")
cat("local demo ok\n")
