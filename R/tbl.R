#' Distributed Dask tables
#'
#' A `tbl_dask` is a lazy Dask DataFrame plus optional grouping columns.
#' `print()` and [head()] preview **the first partition** (Dask's
#' `DataFrame.head`). Use [dplyr::slice_head()] when you need a global
#' first-n (that path collects, then wraps the result).
#'
#' [dplyr::collect()] computes the full frame, resets the pandas index, and
#' returns a tibble.
#'
#' @param x A `tbl_dask`.
#' @param n Number of rows to preview.
#' @param ... Unused.
#' @return `print()` and `head()` return a tibble preview; `collect()` a
#'   tibble of the full compute; `dim()` is `c(NA, ncol)`; `names()` /
#'   `colnames()` a character vector of column names.
#' @name tbl_dask
#' @examples
#' \dontrun{
#' sc <- dask_connect()
#' st <- copy_to(sc, mtcars, overwrite = TRUE)
#' names(st)
#' head(st, 3)
#' dplyr::collect(st)
#' }
NULL

#' @export
#' @rdname tbl_dask
print.tbl_dask <- function(x, n = 6L, ...) {
  nms <- dask_cols(x)
  cat("<tbl_dask> partitions: ", as.integer(x$ddf$npartitions), "\n", sep = "")
  cat("columns: ", paste(nms, collapse = ", "), "\n", sep = "")
  if (length(x$groups)) {
    cat("groups: ", paste(x$groups, collapse = ", "), "\n", sep = "")
  }
  preview <- tryCatch(head.tbl_dask(x, n = n), error = function(e) NULL)
  if (!is.null(preview)) {
    print(preview)
  }
  invisible(x)
}

#' @export
#' @rdname tbl_dask
head.tbl_dask <- function(x, n = 6L, ...) {
  as_tibble(reticulate::py_to_r(x$ddf$head(as.integer(n))))
}

#' @export
#' @rdname tbl_dask
dim.tbl_dask <- function(x) {
  c(NA_integer_, length(dask_cols(x)))
}

#' @method colnames tbl_dask
#' @export
#' @rdname tbl_dask
colnames.tbl_dask <- function(x, ...) {
  dask_cols(x)
}

#' @export
#' @rdname tbl_dask
names.tbl_dask <- function(x) {
  dask_cols(x)
}

#' @export
#' @rdname tbl_dask
collect.tbl_dask <- function(x, ...) {
  py <- reticulate::import_main(convert = FALSE)
  py$`_dasklyr_ddf` <- x$ddf
  pdf <- reticulate::py_eval(
    "_dasklyr_ddf.compute().reset_index(drop=True)",
    convert = TRUE
  )
  as_tibble(pdf)
}
