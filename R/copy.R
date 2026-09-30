#' Copy an R data frame to Dask
#'
#' dplyr [dplyr::copy_to()] method for `dask_connection`. Counterpart to
#' sparklyr's `copy_to.spark_connection`. There is no `dask_copy_to()`;
#' use the dplyr generic, same as sparklyr.
#'
#' Named tables live in a client-side registry (`sc$tables`), not a Hive
#' catalog. Retrieve them with [dplyr::tbl()].
#'
#' @param dest A `dask_connection`.
#' @param df A `data.frame`.
#' @param name Table name in the registry.
#' @param overwrite If `TRUE`, replace an existing name.
#' @param memory If `TRUE`, `persist()` the frame after upload.
#' @param repartition Number of Dask partitions. Same argument name as
#'   sparklyr (not `npartitions`).
#' @param ... Unused.
#' @return A `tbl_dask`.
#' @export
#' @examples
#' \dontrun{
#' sc <- dask_connect()
#' iris_tbl <- copy_to(sc, iris, "iris", overwrite = TRUE)
#' }
copy_to.dask_connection <- function(
  dest,
  df,
  name = deparse(substitute(df)),
  overwrite = FALSE,
  memory = FALSE,
  repartition = 1L,
  ...
) {
  if (!is.data.frame(df)) {
    stop("df must be a data.frame", call. = FALSE)
  }
  name <- dask_sanitize_name(name)
  npart <- as.integer(repartition)
  if (is.na(npart) || npart < 1L) npart <- 1L
  ddf <- dd()$from_pandas(df, npartitions = npart)
  if (isTRUE(memory)) {
    ddf <- ddf$persist()
  }
  dask_register_table(dest, new_tbl_dask(ddf), name, overwrite)
}

#' Retrieve a registered Dask table
#'
#' dplyr [dplyr::tbl()] method for `dask_connection`.
#'
#' @param src A `dask_connection`.
#' @param from Table name previously passed to [copy_to()] or a read helper.
#' @param ... Unused.
#' @return A `tbl_dask`.
#' @export
#' @examples
#' \dontrun{
#' sc <- dask_connect()
#' copy_to(sc, iris, "iris", overwrite = TRUE)
#' tbl(sc, "iris")
#' }
tbl.dask_connection <- function(src, from, ...) {
  if (!exists(from, envir = src$tables, inherits = FALSE)) {
    stop("Unknown table: ", from, call. = FALSE)
  }
  get(from, envir = src$tables, inherits = FALSE)
}
