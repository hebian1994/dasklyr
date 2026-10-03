# Coiled demo. Default path lists clusters and attaches a ready name.
# It does not create a cluster unless DASKLYR_COILED=create.
#
#   pip install coiled
#   coiled login
#   set DASKLYR_PYTHON to that Python
#   set DASKLYR_COILED_NAME to an existing cluster name
#   Rscript examples/coiled_demo.R
#
# Env:
#   DASKLYR_PYTHON / RETICULATE_PYTHON
#   DASKLYR_COILED_NAME     Coiled cluster name (required to connect)
#   DASKLYR_COILED=create   opt-in: may start a new id (costs money)
#   DASKLYR_COILED_WORKERS  used only when creating (default 1)

if (!file.exists("examples/common.R")) {
  stop("Run from the package root: Rscript examples/coiled_demo.R", call. = FALSE)
}
source("examples/common.R")

library(dasklyr)
library(dplyr)

if (!dask_coiled_available()) {
  stop(
    "Python 'coiled' is missing in DASKLYR_PYTHON.\n",
    "  pip install coiled\n",
    "  coiled login",
    call. = FALSE
  )
}

cat("--- Coiled clusters ---\n")
listed <- dask_coiled_clusters()
print(listed)

name <- Sys.getenv("DASKLYR_COILED_NAME", unset = "")
if (!nzchar(name)) {
  stop(
    "Set DASKLYR_COILED_NAME to attach. Default demo will not create a cluster.",
    call. = FALSE
  )
}

ready <- listed[!is.na(listed$name) & listed$name == name & listed$state == "ready", , drop = FALSE]
create <- identical(tolower(Sys.getenv("DASKLYR_COILED", unset = "")), "create")

if (!nrow(ready) && !create) {
  stop(
    "Coiled cluster '", name, "' is not ready. ",
    "Start it on the Coiled site, or set DASKLYR_COILED=create (new id, billed).",
    call. = FALSE
  )
}

sc <- dask_connect(
  master = "coiled",
  cluster_name = name,
  n_workers = if (create) {
    as.integer(Sys.getenv("DASKLYR_COILED_WORKERS", unset = "1"))
  } else {
    NULL
  },
  shutdown_on_close = create
)
on.exit(dask_disconnect(sc), add = TRUE)
print(sc)
cat("dashboard: ", dask_web(sc, browse = FALSE), "\n", sep = "")

tables <- dasklyr_example_copy(sc)
dasklyr_example_dplyr(tables$st, tables$cl)

cat("\n--- after work, list again ---\n")
print(dask_coiled_clusters())
cat("coiled demo ok\n")
