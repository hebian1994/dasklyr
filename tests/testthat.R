library(testthat)

py <- Sys.getenv("DASKLYR_PYTHON", unset = "")
if (!nzchar(py)) py <- Sys.getenv("RETICULATE_PYTHON", unset = "")
if (nzchar(py) && file.exists(py)) {
  reticulate::use_python(py, required = TRUE)
}

library(dasklyr)
test_check("dasklyr")
