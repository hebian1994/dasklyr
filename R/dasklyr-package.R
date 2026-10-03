#' R Interface to Dask
#'
#' Connect to a Dask cluster from R and run dplyr verbs on Dask DataFrames.
#' Computation stays in Python workers; R builds the graph and collects results.
#'
#' Point reticulate at a Python that has Dask with **environment variables**,
#' not hardcoded paths: `DASKLYR_PYTHON` or `RETICULATE_PYTHON`. Cluster
#' address: `DASK_SCHEDULER` (scheduler `tcp://` host and port). Optional
#' Coiled: `master = "coiled"` after `coiled login` / `DASK_COILED__TOKEN`.
#'
#' @seealso [dask_connect()], [copy_to()], [dask_read_parquet()], [ddf_schema()],
#'   [dask_coiled_clusters()]
#' @keywords internal
#' @importFrom dplyr arrange collect copy_to count distinct filter group_by
#'   inner_join left_join mutate n rename select semi_join slice_head src_tbls
#'   summarise summarize tbl ungroup
#' @importFrom rlang :=
#' @importFrom rlang %||%
#' @importFrom rlang enquo enquos get_expr
#' @importFrom stats setNames
#' @importFrom tibble as_tibble
#' @importFrom tidyselect eval_rename eval_select
#' @importFrom utils head
"_PACKAGE"

#' @export
dplyr::copy_to

#' @export
dplyr::tbl

#' @export
dplyr::src_tbls
