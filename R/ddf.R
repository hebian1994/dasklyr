stop_if_not_tbl_dask <- function(x) {
  if (!inherits(x, "tbl_dask")) {
    stop("x is not a tbl_dask", call. = FALSE)
  }
  invisible(x)
}

#' Underlying Dask DataFrame
#'
#' Counterpart to sparklyr's `spark_dataframe()`. Returns the Python object
#' so you can call APIs that dasklyr has not wrapped.
#'
#' @param x A `tbl_dask`.
#' @return A reticulate Python Dask DataFrame.
#' @export
#' @examples
#' \dontrun{
#' sc <- dask_connect()
#' st <- copy_to(sc, mtcars, overwrite = TRUE)
#' dask_dataframe(st)
#' }
dask_dataframe <- function(x) {
  stop_if_not_tbl_dask(x)
  x$ddf
}

#' Table utilities for Dask DataFrames
#'
#' Counterpart to sparklyr's `sdf_*` helpers. Names use `ddf_*` so they are
#' not mistaken for Spark.
#'
#' [ddf_nrow()] computes (`len(ddf)`). [dim()] on a `tbl_dask` reports
#' `NA` rows on purpose; use this when you need the real count.
#'
#' @param x A `tbl_dask`.
#' @param npartitions,partitions Number of partitions. `partitions` is an
#'   alias for `npartitions`.
#' @return `ddf_columns()` a character vector; `ddf_npartitions()` /
#'   `ddf_ncol()` / `ddf_nrow()` integers; `ddf_dim()` `c(nrow, ncol)`;
#'   `ddf_schema()` a tibble of `name` and `type`; `ddf_repartition()` /
#'   `ddf_persist()` the same `tbl_dask` with an updated Python frame.
#' @name ddf_schema
#' @export
#' @examples
#' \dontrun{
#' sc <- dask_connect()
#' st <- copy_to(sc, mtcars, overwrite = TRUE, repartition = 2L)
#' ddf_schema(st)
#' ddf_npartitions(st)
#' ddf_nrow(st)
#' ddf_repartition(st, npartitions = 4L)
#' }
ddf_schema <- function(x) {
  stop_if_not_tbl_dask(x)
  nms <- dask_cols(x)
  types <- vapply(
    nms,
    function(nm) as.character(x$ddf$dtypes[[nm]]),
    character(1),
    USE.NAMES = FALSE
  )
  tibble::tibble(name = nms, type = types)
}

#' @rdname ddf_schema
#' @export
ddf_columns <- function(x) {
  stop_if_not_tbl_dask(x)
  dask_cols(x)
}

#' @rdname ddf_schema
#' @export
ddf_npartitions <- function(x) {
  stop_if_not_tbl_dask(x)
  as.integer(x$ddf$npartitions)
}

#' @rdname ddf_schema
#' @export
ddf_ncol <- function(x) {
  length(ddf_columns(x))
}

#' @rdname ddf_schema
#' @export
ddf_nrow <- function(x) {
  stop_if_not_tbl_dask(x)
  py <- reticulate::import_main(convert = FALSE)
  py$`_dasklyr_ddf` <- x$ddf
  as.integer(reticulate::py_eval("len(_dasklyr_ddf)"))
}

#' @rdname ddf_schema
#' @export
ddf_dim <- function(x) {
  c(ddf_nrow(x), ddf_ncol(x))
}

#' @rdname ddf_schema
#' @export
ddf_repartition <- function(x, npartitions = NULL, partitions = npartitions) {
  stop_if_not_tbl_dask(x)
  n <- npartitions %||% partitions
  if (is.null(n)) {
    stop("ddf_repartition() needs npartitions (alias: partitions)", call. = FALSE)
  }
  x$ddf <- x$ddf$repartition(npartitions = as.integer(n))
  x
}

#' @rdname ddf_schema
#' @export
ddf_persist <- function(x) {
  stop_if_not_tbl_dask(x)
  x$ddf <- x$ddf$persist()
  x
}
