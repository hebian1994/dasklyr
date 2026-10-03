.dask_connections <- new.env(parent = emptyenv())

dask_connections_instances <- function() {
  if (is.null(.dask_connections$instances)) {
    .dask_connections$instances <- list()
  }
  .dask_connections$instances
}

dask_connections_add <- function(sc) {
  instances <- dask_connections_instances()
  instances[[length(instances) + 1L]] <- sc
  .dask_connections$instances <- instances
}

dask_connections_remove <- function(sc) {
  instances <- dask_connections_instances()
  keep <- lapply(instances, function(e) {
    same <- identical(e$master, sc$master) && identical(e$app_name, sc$app_name)
    if (same) NULL else e
  })
  .dask_connections$instances <- Filter(Negate(is.null), keep)
}

#' Find an open Dask connection
#'
#' Counterpart to sparklyr's `spark_connection_find()`.
#'
#' @param master Scheduler address, or `"local"`. `NULL` matches any.
#' @param app_name Application name. `NULL` matches any.
#' @return A list of matching `dask_connection` objects (possibly empty).
#' @export
#' @examples
#' dask_connection_find()
dask_connection_find <- function(master = NULL, app_name = NULL) {
  Filter(
    function(e) {
      dask_connection_is_open(e) &&
        (is.null(master) || identical(e$master, master)) &&
        (is.null(app_name) || identical(e$app_name, app_name))
    },
    dask_connections_instances()
  )
}

#' Is a Dask connection still usable?
#'
#' @param sc A `dask_connection`.
#' @return `TRUE` or `FALSE`.
#' @export
#' @examples
#' dask_connection_is_open(NULL)
dask_connection_is_open <- function(sc) {
  if (!inherits(sc, "dask_connection")) {
    return(FALSE)
  }
  isTRUE(tryCatch(
    {
      sc$client$nthreads()
      TRUE
    },
    error = function(e) FALSE
  ))
}

is_local_master <- function(master) {
  identical(master, "local") || identical(master, "local[*]")
}

#' Connect to Dask
#'
#' Counterpart to sparklyr's `spark_connect(master = ...)`. With no
#' arguments, starts a **local** client. `DASK_SCHEDULER` is not read here;
#' pass `master` (or `dask_connect(Sys.getenv("DASK_SCHEDULER"))`) for a
#' cluster.
#'
#' The same `master` + `app_name` pair reuses an open connection.
#'
#' @param master `"local"` starts a local client. A scheduler address such as
#'   `"tcp://hostname:8786"` connects to a cluster.
#' @param app_name Application name used when reusing connections.
#' @param config A list from [dask_config()].
#' @param timeout Seconds to wait when connecting to a remote scheduler.
#' @param processes Passed to `dask.distributed.Client` for local masters.
#'   Default `FALSE` avoids Windows process-spawn issues.
#' @param scheduler Alias for `master`.
#' @param ... Unused; present for sparklyr-like call compatibility.
#' @return A `dask_connection`.
#' @export
#' @examples
#' \dontrun{
#' sc <- dask_connect(master = "local")
#' sc <- dask_connect(master = Sys.getenv("DASK_SCHEDULER"))
#' }
dask_connect <- function(
  master = "local",
  app_name = "dasklyr",
  config = dask_config(),
  timeout = NULL,
  processes = NULL,
  scheduler = NULL,
  ...
) {
  if (!is.null(scheduler)) {
    master <- scheduler
  }
  if (is.null(master) || !nzchar(master)) {
    master <- "local"
  }
  timeout <- timeout %||% dask_config_value(config, "timeout", 10L)
  processes <- processes %||% dask_config_value(config, "processes", FALSE)

  found <- dask_connection_find(master, app_name)
  if (length(found) == 1L) {
    message("Re-using existing Dask connection to ", master)
    return(found[[1L]])
  }

  dask_require()
  dist <- dask_distributed()
  if (is_local_master(master)) {
    client <- dist$Client(processes = isTRUE(processes))
  } else {
    client <- dist$Client(master, timeout = as.integer(timeout))
  }

  sc <- structure(
    list(
      client = client,
      master = master,
      scheduler = if (is_local_master(master)) NULL else master,
      app_name = app_name,
      config = config,
      tables = new.env(parent = emptyenv())
    ),
    class = "dask_connection"
  )
  dask_connections_add(sc)
  sc
}

#' Disconnect from Dask
#'
#' Counterpart to sparklyr's `spark_disconnect()`.
#'
#' @param sc A `dask_connection`.
#' @param ... Unused.
#' @return `sc`, invisibly.
#' @export
#' @examples
#' \dontrun{
#' sc <- dask_connect()
#' dask_disconnect(sc)
#' }
dask_disconnect <- function(sc, ...) {
  if (!inherits(sc, "dask_connection")) {
    stop("sc is not a dask_connection", call. = FALSE)
  }
  try(sc$client$close(), silent = TRUE)
  dask_connections_remove(sc)
  invisible(sc)
}

#' Disconnect every open Dask connection
#'
#' Counterpart to sparklyr's `spark_disconnect_all()`.
#'
#' @param ... Passed to [dask_disconnect()].
#' @return The number of connections closed.
#' @export
#' @examples
#' dask_disconnect_all()
dask_disconnect_all <- function(...) {
  scons <- dask_connection_find()
  length(lapply(scons, function(e) dask_disconnect(e, ...)))
}

#' Dask dashboard URL
#'
#' Counterpart to sparklyr's `spark_web()`.
#'
#' @param sc A `dask_connection`.
#' @param browse If `TRUE`, open the URL in a browser.
#' @return The dashboard URL, invisibly.
#' @export
#' @examples
#' \dontrun{
#' sc <- dask_connect()
#' dask_web(sc, browse = FALSE)
#' }
dask_web <- function(sc, browse = interactive()) {
  if (!inherits(sc, "dask_connection")) {
    stop("sc is not a dask_connection", call. = FALSE)
  }
  url <- tryCatch(as.character(sc$client$dashboard_link), error = function(e) "")
  if (isTRUE(browse) && nzchar(url)) {
    utils::browseURL(url)
  }
  invisible(url)
}

#' @export
#' @noRd
print.dask_connection <- function(x, ...) {
  cat("<dask_connection>\n")
  cat("  master: ", x$master %||% "(unknown)", "\n", sep = "")
  dash <- tryCatch(
    as.character(x$client$dashboard_link),
    error = function(e) ""
  )
  if (nzchar(dash)) cat("  dashboard: ", dash, "\n", sep = "")
  nthreads <- tryCatch(
    reticulate::py_to_r(x$client$nthreads()),
    error = function(e) NULL
  )
  if (!is.null(nthreads)) {
    cat("  workers: ", length(nthreads), "\n", sep = "")
  }
  invisible(x)
}

#' @export
src_tbls.dask_connection <- function(x, ...) {
  sort(ls(envir = x$tables, all.names = FALSE))
}
