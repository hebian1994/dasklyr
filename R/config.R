#' Runtime configuration for dasklyr
#'
#' Counterpart to sparklyr's `spark_config()`. Values are stored on the
#' connection object returned by [dask_connect()].
#'
#' @param ... Named options. Known keys:
#'   * `timeout`: seconds to wait when connecting to a remote scheduler.
#'   * `processes`: local `Client` processes flag (default `FALSE`).
#'   * `n_workers`: Coiled worker count when creating a cluster.
#'   * `shutdown_on_close`: Coiled cluster teardown on [dask_disconnect()].
#' @return A named list.
#' @export
#' @examples
#' dask_config(timeout = 30)
dask_config <- function(...) {
  cfg <- list(
    timeout = 10L,
    processes = FALSE
  )
  dots <- list(...)
  if (length(dots)) {
    cfg <- utils::modifyList(cfg, dots, keep.null = TRUE)
  }
  cfg
}

dask_config_value <- function(config, name, default = NULL) {
  val <- config[[name]]
  if (is.null(val)) default else val
}
