#' Read CSV or Parquet into a Dask table
#'
#' Counterpart to sparklyr's `spark_read_csv()` / `spark_read_parquet()`.
#' Signature is `(sc, name, path)`: `name` may be omitted and is then derived
#' from the path.
#'
#' `path` must be visible to **workers**. A Windows `tempfile()` is fine for
#' `master = "local"`; it is not usable from a Docker cluster unless the
#' path is a shared volume or object store.
#'
#' @param sc A `dask_connection`.
#' @param name Name to register on `sc`. If `NULL`, derived from `path`.
#' @param path File or glob. Defaults to `name` when you pass a single
#'   path-like argument as sparklyr does.
#' @param memory If `TRUE`, `persist()` after read.
#' @param overwrite If `TRUE`, replace an existing registered name.
#' @param ... Extra arguments forwarded to `dask.dataframe.read_csv` or
#'   `read_parquet`.
#' @return A `tbl_dask`.
#' @export
#' @examples
#' \dontrun{
#' sc <- dask_connect()
#' pq <- dask_read_parquet(sc, "sales", "/data/sales.parquet")
#' }
dask_read_parquet <- function(
  sc,
  name = NULL,
  path = name,
  memory = FALSE,
  overwrite = TRUE,
  ...
) {
  dask_read_file(sc, name, path, memory, overwrite, "read_parquet", ...)
}

#' @rdname dask_read_parquet
#' @export
dask_read_csv <- function(
  sc,
  name = NULL,
  path = name,
  memory = FALSE,
  overwrite = TRUE,
  ...
) {
  dask_read_file(sc, name, path, memory, overwrite, "read_csv", ...)
}

dask_read_file <- function(sc, name, path, memory, overwrite, reader, ...) {
  if (!inherits(sc, "dask_connection")) {
    stop("sc is not a dask_connection", call. = FALSE)
  }
  target <- dask_read_prepare_target(name, path)
  ddf <- dd()[[reader]](path.expand(target$path), ...)
  if (isTRUE(memory)) {
    ddf <- ddf$persist()
  }
  dask_register_table(sc, new_tbl_dask(ddf), target$name, overwrite)
}

#' Write a Dask table to Parquet or CSV
#'
#' Counterpart to sparklyr's `spark_write_parquet()` / `spark_write_csv()`.
#' The destination must be writable from workers.
#'
#' @param x A `tbl_dask`.
#' @param path Destination path.
#' @param mode `"overwrite"` is accepted as an alias for `overwrite = TRUE`.
#' @param overwrite If `TRUE`, replace existing Parquet files.
#' @param ... Extra arguments forwarded to `to_parquet` / `to_csv`.
#' @return `x`, invisibly.
#' @export
#' @examples
#' \dontrun{
#' sc <- dask_connect()
#' st <- copy_to(sc, mtcars, overwrite = TRUE)
#' tmp <- tempfile()
#' dask_write_parquet(st, tmp, overwrite = TRUE)
#' dask_write_csv(st, tempfile())
#' }
dask_write_parquet <- function(x, path, mode = NULL, overwrite = FALSE, ...) {
  if (!inherits(x, "tbl_dask")) {
    stop("x is not a tbl_dask", call. = FALSE)
  }
  path <- path.expand(path)
  if (identical(mode, "overwrite") || isTRUE(overwrite)) {
    x$ddf$to_parquet(path, overwrite = TRUE, ...)
  } else {
    x$ddf$to_parquet(path, ...)
  }
  invisible(x)
}

#' @rdname dask_write_parquet
#' @export
dask_write_csv <- function(x, path, mode = NULL, overwrite = FALSE, ...) {
  if (!inherits(x, "tbl_dask")) {
    stop("x is not a tbl_dask", call. = FALSE)
  }
  x$ddf$to_csv(path.expand(path), ...)
  invisible(x)
}
