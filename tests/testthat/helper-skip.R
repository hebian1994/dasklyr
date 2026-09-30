# Sourced automatically by testthat.
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
