is_coiled_master <- function(master) {
  identical(as.character(master)[[1L]], "coiled")
}

dask_coiled_normalize_name <- function(cluster_name) {
  if (is.null(cluster_name) || !nzchar(as.character(cluster_name)[[1L]])) {
    stop(
      "master = \"coiled\" requires cluster_name.\n",
      "This is the Coiled cluster name, not app_name.",
      call. = FALSE
    )
  }
  as.character(cluster_name)[[1L]]
}

#' Is the Python coiled package available?
#'
#' Checks that reticulate can import `coiled`. Honors `DASKLYR_PYTHON`
#' if set. Does not start a cluster.
#'
#' @return `TRUE` or `FALSE`.
#' @export
#' @examples
#' \dontrun{
#' dask_coiled_available()
#' # pip install coiled && coiled login  (same Python as DASKLYR_PYTHON)
#' }
dask_coiled_available <- function() {
  isTRUE(tryCatch(
    {
      dask_bind_python()
      reticulate::py_available(initialize = TRUE) &&
        reticulate::py_module_available("coiled")
    },
    error = function(e) FALSE
  ))
}

dask_require_coiled <- function() {
  dask_require()
  if (!reticulate::py_module_available("coiled")) {
    stop(
      "Python package 'coiled' is missing. In the same Python as DASKLYR_PYTHON:\n",
      "  pip install coiled\n",
      "  coiled login",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

dask_coiled <- function() {
  dask_require_coiled()
  reticulate::import("coiled")
}

dask_coiled_rows_to_tibble <- function(rows) {
  empty <- tibble::tibble(
    id = integer(),
    name = character(),
    state = character()
  )
  if (is.null(rows)) {
    return(empty)
  }
  if (is.data.frame(rows)) {
    id <- if ("id" %in% names(rows)) as.integer(rows$id) else integer(nrow(rows))
    name <- if ("name" %in% names(rows)) as.character(rows$name) else
      rep(NA_character_, nrow(rows))
    state <- if ("state" %in% names(rows)) {
      as.character(rows$state)
    } else if ("current_state" %in% names(rows)) {
      vapply(rows$current_state, dask_coiled_row_state, character(1))
    } else {
      rep(NA_character_, nrow(rows))
    }
    return(tibble::tibble(id = id, name = name, state = state))
  }
  if (!is.list(rows) || !length(rows)) {
    return(empty)
  }
  tibble::tibble(
    id = vapply(rows, function(r) {
      val <- r[["id"]]
      if (is.null(val)) NA_integer_ else as.integer(val)[[1L]]
    }, integer(1)),
    name = vapply(rows, function(r) {
      val <- r[["name"]]
      if (is.null(val)) NA_character_ else as.character(val)[[1L]]
    }, character(1)),
    state = vapply(rows, dask_coiled_row_state, character(1))
  )
}

dask_coiled_row_state <- function(row) {
  if (is.null(row)) {
    return(NA_character_)
  }
  cs <- row[["current_state"]]
  if (is.list(cs) && !is.null(cs[["state"]])) {
    return(as.character(cs[["state"]])[[1L]])
  }
  if (!is.null(row[["state"]])) {
    return(as.character(row[["state"]])[[1L]])
  }
  NA_character_
}

#' List Coiled clusters for this account
#'
#' Thin wrapper around `coiled.list_clusters()`. Requires the Python
#' `coiled` package and a token from `coiled login` or
#' `DASK_COILED__TOKEN`.
#'
#' @return A tibble with `id`, `name`, and `state`.
#' @export
#' @examples
#' \dontrun{
#' dask_coiled_clusters()
#' sc <- dask_connect(
#'   master = "coiled",
#'   cluster_name = Sys.getenv("DASKLYR_COILED_NAME"),
#'   n_workers = 1L,
#'   shutdown_on_close = FALSE
#' )
#' dask_disconnect(sc)
#' }
dask_coiled_clusters <- function() {
  coiled <- dask_coiled()
  rows <- tryCatch(
    reticulate::py_to_r(coiled$list_clusters()),
    error = function(e) {
      stop(
        "Could not list Coiled clusters. Run `coiled login` or set DASK_COILED__TOKEN.\n",
        conditionMessage(e),
        call. = FALSE
      )
    }
  )
  dask_coiled_rows_to_tibble(rows)
}

dask_coiled_ready_id <- function(cluster_name, clusters = NULL) {
  if (is.null(clusters)) {
    clusters <- dask_coiled_clusters()
  }
  ready <- clusters[
    !is.na(clusters$name) &
      clusters$name == cluster_name &
      !is.na(clusters$state) &
      clusters$state == "ready",
    ,
    drop = FALSE
  ]
  if (!nrow(ready)) {
    return(NULL)
  }
  as.integer(ready$id[[1L]])
}

# Attach vs create, without talking to Coiled. Live tests must not create
# a cluster unless DASKLYR_COILED=create.
dask_coiled_plan <- function(cluster_name, clusters) {
  cluster_name <- as.character(cluster_name)[[1L]]
  ready_id <- dask_coiled_ready_id(cluster_name, clusters)
  same <- clusters[
    !is.na(clusters$name) & clusters$name == cluster_name,
    ,
    drop = FALSE
  ]
  if (!is.null(ready_id)) {
    return(list(
      action = "attach",
      coiled_id = ready_id,
      pass_n_workers = FALSE,
      message = paste0(
        "Attaching to Coiled cluster '", cluster_name, "' (id ", ready_id, ")"
      )
    ))
  }
  if (nrow(same)) {
    states <- paste(unique(same$state), collapse = ", ")
    return(list(
      action = "create",
      coiled_id = NULL,
      pass_n_workers = TRUE,
      message = paste0(
        "Coiled cluster '", cluster_name, "' exists but is not ready (",
        states, "); creating a new cluster id"
      )
    ))
  }
  list(
    action = "create",
    coiled_id = NULL,
    pass_n_workers = TRUE,
    message = paste0("Creating Coiled cluster '", cluster_name, "'")
  )
}

dask_coiled_cluster_id <- function(cluster, cluster_name) {
  id <- tryCatch(
    reticulate::py_to_r(cluster$cluster_id),
    error = function(e) NULL
  )
  if (!is.null(id) && !is.na(id) && length(id)) {
    return(as.integer(id)[[1L]])
  }
  dask_coiled_ready_id(cluster_name)
}

dask_coiled_open <- function(cluster_name, n_workers = NULL, shutdown_on_close = FALSE) {
  coiled <- dask_coiled()
  plan <- dask_coiled_plan(cluster_name, dask_coiled_clusters())
  message(plan$message)

  kwargs <- list(
    name = cluster_name,
    shutdown_on_close = isTRUE(shutdown_on_close)
  )
  if (isTRUE(plan$pass_n_workers)) {
    kwargs$n_workers <- as.integer(n_workers %||% 1L)
  }

  cluster <- tryCatch(
    do.call(coiled$Cluster, kwargs),
    error = function(e) {
      stop(
        "Could not open a Coiled cluster.\n",
        "Install coiled (`pip install coiled`) and run `coiled login`, ",
        "or set DASK_COILED__TOKEN.\n",
        conditionMessage(e),
        call. = FALSE
      )
    }
  )
  client <- cluster$get_client()
  list(
    cluster = cluster,
    client = client,
    coiled_id = dask_coiled_cluster_id(cluster, cluster_name)
  )
}

dask_coiled_close <- function(sc) {
  try(sc$client$close(), silent = TRUE)
  if (is.null(sc$coiled_cluster)) {
    return(invisible(sc))
  }
  try(
    sc$coiled_cluster$close(force_shutdown = isTRUE(sc$shutdown_on_close)),
    silent = TRUE
  )
  invisible(sc)
}
