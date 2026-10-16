call_fun_name <- function(e) {
  f <- e[[1]]
  if (is.symbol(f)) return(as.character(f))
  if (is.call(f) && identical(f[[1]], quote(`::`)) && length(f) == 3) {
    return(as.character(f[[3]]))
  }
  NA_character_
}

query_literal <- function(e) {
  if (is.character(e)) {
    return(paste0("'", gsub("'", "\\\\'", e, fixed = TRUE), "'"))
  }
  if (is.logical(e) && length(e) == 1L) {
    return(if (isTRUE(e)) "True" else "False")
  }
  as.character(e)
}

in_literals <- function(e) {
  if (rlang::is_syntactic_literal(e) && length(e) == 1L) {
    return(query_literal(e))
  }
  if (is.call(e) && identical(e[[1]], quote(c))) {
    vals <- lapply(as.list(e)[-1], function(x) {
      if (!rlang::is_syntactic_literal(x)) {
        stop("%in% on tbl_dask needs a vector of literals.", call. = FALSE)
      }
      query_literal(x)
    })
    return(paste(vals, collapse = ", "))
  }
  stop("%in% on tbl_dask needs a vector of literals.", call. = FALSE)
}

expr_to_query <- function(e) {
  if (rlang::is_syntactic_literal(e)) {
    return(query_literal(e))
  }
  if (is.symbol(e)) {
    return(as.character(e))
  }
  if (!is.call(e)) {
    stop("Unsupported expression: ", paste(deparse(e), collapse = " "), call. = FALSE)
  }
  fun <- call_fun_name(e)
  if (identical(as.character(e[[1]]), "(") && length(e) == 2) {
    return(paste0("(", expr_to_query(e[[2]]), ")"))
  }
  if (identical(fun, "is.na") && length(e) == 2) {
    return(paste0(expr_to_query(e[[2]]), ".isnull()"))
  }
  if (identical(fun, "!") && length(e) == 2) {
    return(paste0("not (", expr_to_query(e[[2]]), ")"))
  }
  if (identical(fun, "%in%") && length(e) == 3) {
    return(paste0(expr_to_query(e[[2]]), ".isin([", in_literals(e[[3]]), "])"))
  }
  bin <- c(
    `+` = "+", `-` = "-", `*` = "*", `/` = "/",
    `>` = ">", `<` = "<", `>=` = ">=", `<=` = "<=",
    `==` = "==", `!=` = "!=",
    `&` = "and", `|` = "or", `&&` = "and", `||` = "or"
  )
  op <- as.character(e[[1]])
  if (op %in% names(bin) && length(e) == 3) {
    return(paste(expr_to_query(e[[2]]), bin[[op]], expr_to_query(e[[3]])))
  }
  stop(
    "This dplyr verb only supports simple column comparisons on tbl_dask.\n",
    "Got: ", paste(deparse(e), collapse = " "),
    call. = FALSE
  )
}

eval_dask_expr <- function(ddf, e) {
  if (rlang::is_syntactic_literal(e) && length(e) == 1L) {
    return(e)
  }
  if (is.symbol(e)) {
    return(ddf[[as.character(e)]])
  }
  if (!is.call(e)) {
    stop("Unsupported mutate expression: ", paste(deparse(e), collapse = " "), call. = FALSE)
  }
  if (identical(as.character(e[[1]]), "(") && length(e) == 2) {
    return(eval_dask_expr(ddf, e[[2]]))
  }
  op <- as.character(e[[1]])
  if (identical(op, "-") && length(e) == 2) {
    return(-eval_dask_expr(ddf, e[[2]]))
  }
  if (op %in% c("+", "-", "*", "/") && length(e) == 3) {
    left <- eval_dask_expr(ddf, e[[2]])
    right <- eval_dask_expr(ddf, e[[3]])
    return(switch(
      op,
      `+` = left + right,
      `-` = left - right,
      `*` = left * right,
      `/` = left / right
    ))
  }
  stop(
    "mutate() on tbl_dask supports copying a column, literals, and + - * /.",
    call. = FALSE
  )
}

parse_agg <- function(e) {
  if (is.call(e)) {
    fun <- call_fun_name(e)
    if (identical(fun, "n") && length(e) == 1) {
      return(list(fun = "size", col = NA_character_))
    }
    if (length(e) >= 2 && is.symbol(e[[2]])) {
      col <- as.character(e[[2]])
      if (identical(fun, "n_distinct")) {
        return(list(fun = "nunique", col = col))
      }
      if (fun %in% c("mean", "sum", "min", "max", "count")) {
        return(list(fun = fun, col = col))
      }
    }
  }
  stop(
    "summarise() on tbl_dask supports n(), n_distinct(col), mean/sum/min/max/count(col).",
    call. = FALSE
  )
}

dask_assign_col <- function(ddf, nm, val) {
  args <- list(val)
  names(args) <- nm
  do.call(ddf$assign, args)
}

normalize_join_by <- function(x, y, by) {
  if (is.null(by)) {
    by <- intersect(dask_cols(x), dask_cols(y))
  }
  if (!length(by)) {
    stop("join could not find a `by` column", call. = FALSE)
  }
  nms <- names(by)
  if (is.null(nms)) {
    by <- unname(as.character(by))
    return(list(left = by, right = by, same = TRUE))
  }
  left <- ifelse(nms == "", unname(as.character(by)), nms)
  right <- unname(as.character(by))
  list(left = left, right = right, same = identical(left, right))
}

dask_merge_tbl <- function(x, y, by, how, suffix) {
  if (!inherits(y, "tbl_dask")) {
    stop("join on tbl_dask needs a tbl_dask on the right", call. = FALSE)
  }
  keys <- normalize_join_by(x, y, by)
  if (isTRUE(keys$same)) {
    x$ddf <- x$ddf$merge(
      y$ddf,
      on = as.list(keys$left),
      how = how,
      suffixes = as.list(suffix)
    )
  } else {
    x$ddf <- x$ddf$merge(
      y$ddf,
      left_on = as.list(keys$left),
      right_on = as.list(keys$right),
      how = how,
      suffixes = as.list(suffix)
    )
  }
  x$groups <- character()
  x
}

#' @export
select.tbl_dask <- function(.data, ...) {
  nms <- dask_cols(.data)
  loc <- eval_select(rlang::expr(c(...)), setNames(nms, nms))
  cols <- unname(nms[loc])
  .data$ddf <- dask_pick_cols(.data$ddf, cols)
  .data$groups <- intersect(.data$groups, cols)
  .data
}

#' @export
rename.tbl_dask <- function(.data, ...) {
  nms <- dask_cols(.data)
  loc <- tidyselect::eval_rename(rlang::expr(c(...)), setNames(nms, nms))
  old <- nms[as.integer(loc)]
  new <- names(loc)
  rename_map <- setNames(as.list(new), old)
  .data$ddf <- .data$ddf$rename(columns = rename_map)
  g <- .data$groups
  for (i in seq_along(old)) {
    g[g == old[[i]]] <- new[[i]]
  }
  .data$groups <- g
  .data
}

#' @export
filter.tbl_dask <- function(.data, ..., .by = NULL, .preserve = FALSE) {
  if (!is.null(.by)) {
    stop("filter(.by =) is not supported on tbl_dask", call. = FALSE)
  }
  dots <- enquos(...)
  if (!length(dots)) return(.data)
  parts <- vapply(dots, function(q) expr_to_query(get_expr(q)), character(1))
  qstr <- paste(paste0("(", parts, ")"), collapse = " and ")
  .data$ddf <- .data$ddf$query(qstr, engine = "python")
  .data
}

#' @export
mutate.tbl_dask <- function(.data, ...) {
  dots <- enquos(..., .named = TRUE)
  ddf <- .data$ddf
  for (nm in names(dots)) {
    val <- eval_dask_expr(ddf, get_expr(dots[[nm]]))
    ddf <- dask_assign_col(ddf, nm, val)
  }
  .data$ddf <- ddf
  .data
}

#' @export
arrange.tbl_dask <- function(.data, ...) {
  dots <- enquos(...)
  if (!length(dots)) return(.data)
  by <- character()
  ascending <- logical()
  for (q in dots) {
    e <- get_expr(q)
    if (is.call(e) && identical(call_fun_name(e), "desc") && is.symbol(e[[2]])) {
      by <- c(by, as.character(e[[2]]))
      ascending <- c(ascending, FALSE)
    } else if (is.symbol(e)) {
      by <- c(by, as.character(e))
      ascending <- c(ascending, TRUE)
    } else {
      stop("arrange() on tbl_dask supports columns and desc(col).", call. = FALSE)
    }
  }
  .data$ddf <- .data$ddf$sort_values(by = as.list(by), ascending = as.list(ascending))
  .data
}

#' @export
group_by.tbl_dask <- function(.data, ..., .add = FALSE, .drop = TRUE) {
  nms <- dask_cols(.data)
  loc <- eval_select(rlang::expr(c(...)), setNames(nms, nms))
  grps <- nms[loc]
  if (isTRUE(.add)) grps <- union(.data$groups, grps)
  .data$groups <- grps
  .data
}

#' @export
ungroup.tbl_dask <- function(x, ...) {
  x$groups <- character()
  x
}

#' @export
summarise.tbl_dask <- function(.data, ..., .groups = "drop") {
  dots <- enquos(..., .named = TRUE)
  if (!length(dots)) {
    stop("summarise() needs at least one aggregate", call. = FALSE)
  }
  parsed <- lapply(dots, function(q) parse_agg(get_expr(q)))
  names(parsed) <- names(dots)

  ddf <- .data$ddf
  groups <- .data$groups
  dummy <- FALSE
  if (!length(groups)) {
    dummy <- TRUE
    ddf <- dask_assign_col(ddf, "_dasklyr_g", 0L)
    groups <- "_dasklyr_g"
  }

  spec <- list()
  nunique_map <- list()
  need_ones <- FALSE
  for (nm in names(parsed)) {
    p <- parsed[[nm]]
    if (identical(p$fun, "size")) {
      need_ones <- TRUE
      spec[[nm]] <- c("_dasklyr_n", "sum")
    } else if (identical(p$fun, "nunique")) {
      nunique_map[[nm]] <- p$col
    } else {
      spec[[nm]] <- c(p$col, p$fun)
    }
  }
  if (!length(spec) && !length(nunique_map)) {
    stop("summarise() produced no aggregates", call. = FALSE)
  }
  if (need_ones) {
    ddf <- dask_assign_col(ddf, "_dasklyr_n", 1L)
  }
  out <- dask_summarise_agg(ddf, groups, spec, nunique_map)
  tbl <- new_tbl_dask(out, groups = character())
  if (dummy) {
    cols <- setdiff(dask_cols(tbl), "_dasklyr_g")
    tbl$ddf <- dask_pick_cols(tbl$ddf, cols)
  }
  tbl
}

#' @export
summarize.tbl_dask <- summarise.tbl_dask

#' @export
count.tbl_dask <- function(x, ..., wt = NULL, sort = FALSE, name = NULL, .drop = TRUE) {
  wt_expr <- get_expr(enquo(wt))
  if (!is.null(wt_expr) && !identical(wt_expr, quote(NULL))) {
    stop("count(wt=) is not supported on tbl_dask", call. = FALSE)
  }
  nm <- name %||% "n"
  out <- summarise(group_by(x, ...), n = dplyr::n(), .groups = "drop")
  if (!identical(nm, "n")) {
    out$ddf <- out$ddf$rename(columns = setNames(list(nm), "n"))
  }
  if (isTRUE(sort)) {
    out$ddf <- out$ddf$sort_values(by = nm, ascending = FALSE)
  }
  out
}

#' @export
distinct.tbl_dask <- function(.data, ..., .keep_all = FALSE) {
  dots <- enquos(...)
  if (length(dots)) {
    nms <- dask_cols(.data)
    loc <- eval_select(rlang::expr(c(...)), setNames(nms, nms))
    cols <- unname(nms[loc])
    if (isTRUE(.keep_all)) {
      .data$ddf <- .data$ddf$drop_duplicates(subset = as.list(cols))
    } else {
      .data$ddf <- dask_pick_cols(.data$ddf, cols)$drop_duplicates()
    }
  } else {
    .data$ddf <- .data$ddf$drop_duplicates()
  }
  .data$groups <- character()
  .data
}

#' @export
slice_head.tbl_dask <- function(.data, ..., n, prop, by = NULL) {
  if (!missing(prop)) {
    stop("slice_head(prop=) is not supported on tbl_dask", call. = FALSE)
  }
  if (!is.null(by)) {
    stop("slice_head(by=) is not supported on tbl_dask", call. = FALSE)
  }
  if (missing(n)) n <- 5L
  pdf <- collect.tbl_dask(.data)
  pdf <- utils::head(pdf, as.integer(n))
  .data$ddf <- dd()$from_pandas(as.data.frame(pdf), npartitions = 1L)
  .data
}

#' @export
left_join.tbl_dask <- function(x, y, by = NULL, copy = FALSE, suffix = c(".x", ".y"), ...) {
  dask_merge_tbl(x, y, by, "left", suffix)
}

#' @export
inner_join.tbl_dask <- function(x, y, by = NULL, copy = FALSE, suffix = c(".x", ".y"), ...) {
  dask_merge_tbl(x, y, by, "inner", suffix)
}

#' @export
semi_join.tbl_dask <- function(x, y, by = NULL, copy = FALSE, ...) {
  if (!inherits(y, "tbl_dask")) {
    stop("join on tbl_dask needs a tbl_dask on the right", call. = FALSE)
  }
  keys <- normalize_join_by(x, y, by)
  yk <- dask_pick_cols(y$ddf, keys$right)$drop_duplicates()
  if (isTRUE(keys$same)) {
    x$ddf <- x$ddf$merge(yk, on = as.list(keys$left), how = "inner")
  } else {
    x$ddf <- x$ddf$merge(
      yk,
      left_on = as.list(keys$left),
      right_on = as.list(keys$right),
      how = "inner"
    )
    drop <- setdiff(keys$right, keys$left)
    keep <- setdiff(as.character(reticulate::py_to_r(x$ddf$columns$to_list())), drop)
    x$ddf <- dask_pick_cols(x$ddf, keep)
  }
  x$groups <- character()
  x
}
