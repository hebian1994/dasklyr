is_dask_dataframe <- function(x) {
  inherits(x, "python.builtin.object") &&
    isTRUE(reticulate::py_has_attr(x, "npartitions")) &&
    isTRUE(reticulate::py_has_attr(x, "columns"))
}

dask_py_call <- function(obj, method, ...) {
  if (!nzchar(method)) {
    stop("method must be a non-empty string", call. = FALSE)
  }
  fn <- reticulate::py_get_attr(obj, method)
  fn(...)
}

#' Invoke a Python method
#'
#' Counterpart to sparklyr's `invoke()`: call a method on a Dask DataFrame
#' or on the distributed `Client`. If the result looks like a Dask DataFrame
#' it is wrapped as a `tbl_dask`.
#'
#' @param x A `tbl_dask` or `dask_connection`.
#' @param method Python method name.
#' @param ... Arguments forwarded to the Python method.
#' @return A `tbl_dask` if the Python result looks like a Dask DataFrame;
#'   otherwise the value returned by reticulate.
#' @export
#' @examples
#' \dontrun{
#' sc <- dask_connect()
#' st <- copy_to(sc, mtcars, overwrite = TRUE)
#' dask_invoke(st, "npartitions")
#' }
dask_invoke <- function(x, method, ...) {
  UseMethod("dask_invoke")
}

#' @export
#' @rdname dask_invoke
dask_invoke.tbl_dask <- function(x, method, ...) {
  res <- dask_py_call(x$ddf, method, ...)
  if (is_dask_dataframe(res)) {
    x$ddf <- res
    return(x)
  }
  res
}

#' @export
#' @rdname dask_invoke
dask_invoke.dask_connection <- function(x, method, ...) {
  dask_py_call(x$client, method, ...)
}
