# Sourced automatically by testthat.
#
# Live Coiled never creates a cluster unless DASKLYR_COILED=create.
#   unset / missing coiled → skip
#   DASKLYR_COILED=list     → list_clusters only
#   DASKLYR_COILED=true|attach + DASKLYR_COILED_NAME ready → attach
#   DASKLYR_COILED=create + DASKLYR_COILED_NAME → may start a new id

skip_if_no_dask <- function() {
  skip_on_cran()
  py <- Sys.getenv("DASKLYR_PYTHON", unset = "")
  if (!nzchar(py)) py <- Sys.getenv("RETICULATE_PYTHON", unset = "")
  if (nzchar(py) && file.exists(py)) {
    reticulate::use_python(py, required = TRUE)
  }
  if (!dask_available()) skip("dask Python package not available")
}

skip_if_no_cluster <- function() {
  skip_if_no_dask()
  scheduler <- Sys.getenv("DASK_SCHEDULER", unset = "")
  if (!nzchar(scheduler)) skip("DASK_SCHEDULER is not set")
  scheduler
}

dasklyr_coiled_flag <- function() {
  tolower(Sys.getenv("DASKLYR_COILED", unset = ""))
}

skip_if_no_coiled_module <- function() {
  skip_if_no_dask()
  if (!dask_coiled_available()) skip("coiled Python package not available")
}

skip_if_no_coiled_list <- function() {
  skip_if_no_coiled_module()
  if (!nzchar(dasklyr_coiled_flag())) skip("DASKLYR_COILED is not set")
}

skip_if_no_coiled_attach <- function() {
  skip_if_no_coiled_list()
  flag <- dasklyr_coiled_flag()
  if (!flag %in% c("true", "attach", "1", "yes")) {
    skip("DASKLYR_COILED is not attach/true")
  }
  name <- Sys.getenv("DASKLYR_COILED_NAME", unset = "")
  if (!nzchar(name)) skip("DASKLYR_COILED_NAME is not set")
  clusters <- tryCatch(
    dask_coiled_clusters(),
    error = function(e) e
  )
  if (inherits(clusters, "error")) {
    skip(paste("coiled list failed:", conditionMessage(clusters)))
  }
  id <- dasklyr:::dask_coiled_ready_id(name, clusters)
  if (is.null(id)) {
    skip(paste0("Coiled cluster '", name, "' is not ready; will not create one"))
  }
  list(name = name, coiled_id = id)
}

skip_if_no_coiled_create <- function() {
  skip_if_no_coiled_list()
  if (!identical(dasklyr_coiled_flag(), "create")) {
    skip("DASKLYR_COILED is not create")
  }
  name <- Sys.getenv("DASKLYR_COILED_NAME", unset = "")
  if (!nzchar(name)) skip("DASKLYR_COILED_NAME is not set")
  name
}
