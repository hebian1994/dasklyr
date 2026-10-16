dask_bind_python <- function() {
  if (isTRUE(reticulate::py_available(initialize = FALSE))) {
    return(invisible(NULL))
  }
  py <- Sys.getenv("DASKLYR_PYTHON", unset = "")
  if (!nzchar(py)) {
    return(invisible(NULL))
  }
  if (!file.exists(py)) {
    stop("DASKLYR_PYTHON does not exist: ", py, call. = FALSE)
  }
  reticulate::use_python(py, required = TRUE)
  invisible(py)
}

dask_require <- function() {
  dask_bind_python()
  if (!reticulate::py_available(initialize = TRUE)) {
    stop(
      "Python is not available to reticulate.\n",
      "Set DASKLYR_PYTHON or RETICULATE_PYTHON to a Python that has dask.",
      call. = FALSE
    )
  }
  if (!reticulate::py_module_available("dask.dataframe")) {
    stop(
      "Python package 'dask' is missing. Create an env and run:\n",
      "  pip install \"dask[dataframe,distributed]\" pyarrow pandas",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' Is Dask available to this R session?
#'
#' Checks that reticulate can start Python and import `dask.dataframe`.
#' Honors `DASKLYR_PYTHON` if set.
#'
#' @return `TRUE` or `FALSE`.
#' @export
#' @examples
#' \dontrun{
#' dask_available()
#' }
dask_available <- function() {
  isTRUE(tryCatch(
    {
      dask_bind_python()
      reticulate::py_available(initialize = TRUE) &&
        reticulate::py_module_available("dask.dataframe")
    },
    error = function(e) FALSE
  ))
}

dd <- function() {
  dask_require()
  reticulate::import("dask.dataframe")
}

# One groupby.agg() for n()/mean/sum/... so we do not merge a size()
# frame onto an agg frame. That merge KeyErrors after hash-join shuffle
# (pandas 3 string index labels vs the post-shuffle columns).
DASKLYR_SUMMARISE_PY <- paste(
  "def _dasklyr_summarise(ddf, groups, spec, nunique):",
  "    import dask.dataframe as dd",
  "    groups = [str(g) for g in list(groups)]",
  "    kw = {}",
  "    if spec is not None:",
  "        for k, v in dict(spec).items():",
  "            kw[str(k)] = (str(v[0]), str(v[1]))",
  "    nu = {}",
  "    if nunique is not None:",
  "        for k, v in dict(nunique).items():",
  "            nu[str(k)] = str(v)",
  "    if kw and nu:",
  "        ddf = ddf.persist()",
  "    out = None",
  "    if kw:",
  "        out = ddf.groupby(groups).agg(**kw).reset_index()",
  "    if not nu:",
  "        return out",
  "    extra_pd = None",
  "    extras = []",
  "    for name, col in nu.items():",
  "        piece = (",
  "            ddf.groupby(groups)[col].nunique()",
  "            .to_frame(name)",
  "            .reset_index()",
  "        )",
  "        extras.append(piece)",
  "    if out is None:",
  "        acc = extras[0]",
  "        for piece in extras[1:]:",
  "            acc = dd.merge(acc, piece, on=groups, how='left')",
  "        return acc",
  "    extra_pd = extras[0].compute()",
  "    for piece in extras[1:]:",
  "        extra_pd = extra_pd.merge(piece.compute(), on=groups, how='left')",
  "    def _m(pdf, extra=extra_pd, on=groups):",
  "        return pdf.merge(extra, on=on, how='left')",
  "    return out.map_partitions(_m, meta=_m(out._meta))",
  sep = "\n"
)

dask_ensure_summarise_py <- function() {
  dask_require()
  main <- reticulate::import_main(convert = FALSE)
  if (isTRUE(reticulate::py_has_attr(main, "_dasklyr_summarise"))) {
    return(invisible(NULL))
  }
  reticulate::py_run_string(DASKLYR_SUMMARISE_PY)
  invisible(NULL)
}

dask_summarise_agg <- function(ddf, groups, spec, nunique) {
  dask_ensure_summarise_py()
  main <- reticulate::import_main(convert = FALSE)
  main$`_dasklyr_summarise`(
    ddf,
    as.list(unname(as.character(groups))),
    if (length(spec)) spec else NULL,
    if (length(nunique)) nunique else NULL
  )
}

dask_distributed <- function() {
  dask_require()
  reticulate::import("dask.distributed")
}

dask_cols <- function(x) {
  nms <- reticulate::py_to_r(x$ddf$columns$to_list())
  as.character(nms)
}

dask_pick_cols <- function(ddf, cols) {
  ddf[reticulate::r_to_py(as.list(unname(as.character(cols))))]
}

new_tbl_dask <- function(ddf, groups = character()) {
  structure(
    list(ddf = ddf, groups = as.character(groups)),
    class = c("tbl_dask", "tbl")
  )
}

dask_sanitize_name <- function(name) {
  name <- as.character(name)[[1L]]
  name <- gsub("\\s+", "_", name, perl = TRUE)
  name <- gsub("[^A-Za-z0-9_]", "", name, perl = TRUE)
  if (!nzchar(name)) "df" else name
}

dask_name_from_path <- function(path) {
  base <- tools::file_path_sans_ext(basename(path.expand(as.character(path)[[1L]])))
  dask_sanitize_name(base)
}

dask_register_table <- function(sc, tbl, name, overwrite = TRUE) {
  name <- dask_sanitize_name(name)
  if (exists(name, envir = sc$tables, inherits = FALSE) && !isTRUE(overwrite)) {
    stop(
      "table ", name, " already exists (pass overwrite = TRUE to overwrite)",
      call. = FALSE
    )
  }
  assign(name, tbl, envir = sc$tables)
  tbl
}

dask_read_prepare_target <- function(name, path) {
  if (is.null(name) && is.null(path)) {
    stop("The 'path' parameter must be specified.", call. = FALSE)
  }
  if (is.null(path) || identical(name, path)) {
    path <- name
    name <- dask_name_from_path(path)
  } else if (is.null(name)) {
    name <- dask_name_from_path(path)
  }
  list(name = as.character(name)[[1L]], path = path)
}
