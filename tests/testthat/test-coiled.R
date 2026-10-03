# Layer 0: no Python, no Coiled account.
# Layer 1: DASKLYR_COILED set → list_clusters only.
# Layer 2: DASKLYR_COILED=true|attach + ready DASKLYR_COILED_NAME → attach.
# Layer 3: DASKLYR_COILED=create → may start a new id (paid).

test_that("coiled master requires cluster_name, not app_name", {
  expect_error(dask_connect(master = "coiled"), "cluster_name")
  expect_error(dask_connect(master = "coiled", cluster_name = ""), "cluster_name")
  expect_error(
    dask_connect(master = "coiled", app_name = "demo-keep"),
    "cluster_name"
  )
  expect_error(
    dasklyr:::dask_coiled_normalize_name(NULL),
    "cluster_name"
  )
})

test_that("is_coiled_master is exact", {
  expect_true(dasklyr:::is_coiled_master("coiled"))
  expect_false(dasklyr:::is_coiled_master("local"))
  expect_false(dasklyr:::is_coiled_master("tcp://127.0.0.1:8786"))
  expect_false(dasklyr:::is_coiled_master("tls://10.0.0.1:8786"))
})

test_that("R-session reuse key is cluster_name for coiled", {
  a <- list(master = "coiled", cluster_name = "demo-keep", app_name = "one")
  b <- list(master = "coiled", cluster_name = "demo-keep", app_name = "two")
  c <- list(master = "coiled", cluster_name = "other", app_name = "one")
  local_a <- list(master = "local", app_name = "one")
  local_b <- list(master = "local", app_name = "one")
  expect_true(dasklyr:::dask_connections_same(a, b))
  expect_false(dasklyr:::dask_connections_same(a, c))
  expect_false(dasklyr:::dask_connections_same(a, local_a))
  expect_true(dasklyr:::dask_connections_same(local_a, local_b))
})

test_that("coiled list helper flattens current_state", {
  rows <- list(
    list(id = 2150039L, name = "demo-keep", current_state = list(state = "stopped")),
    list(id = 2150050L, name = "demo-keep", current_state = list(state = "ready"))
  )
  got <- dasklyr:::dask_coiled_rows_to_tibble(rows)
  expect_equal(got$id, c(2150039L, 2150050L))
  expect_equal(got$state, c("stopped", "ready"))
  expect_equal(nrow(dasklyr:::dask_coiled_rows_to_tibble(list())), 0)
  expect_equal(nrow(dasklyr:::dask_coiled_rows_to_tibble(NULL)), 0)

  as_df <- data.frame(
    id = 1L,
    name = "x",
    state = "ready",
    stringsAsFactors = FALSE
  )
  expect_equal(dasklyr:::dask_coiled_rows_to_tibble(as_df)$state, "ready")
})

test_that("coiled plan attaches ready name and creates only when not ready", {
  clusters <- dasklyr_coiled_fixture_clusters()

  attach <- dasklyr:::dask_coiled_plan("demo-keep", clusters)
  expect_equal(attach$action, "attach")
  expect_equal(attach$coiled_id, 2150050L)
  expect_false(attach$pass_n_workers)
  expect_match(attach$message, "2150050")

  stopped_only <- clusters[clusters$state != "ready", , drop = FALSE]
  recreate <- dasklyr:::dask_coiled_plan("demo-keep", stopped_only)
  expect_equal(recreate$action, "create")
  expect_true(recreate$pass_n_workers)
  expect_null(recreate$coiled_id)
  expect_match(recreate$message, "not ready")

  missing <- dasklyr:::dask_coiled_plan("brand-new", clusters)
  expect_equal(missing$action, "create")
  expect_true(missing$pass_n_workers)
  expect_match(missing$message, "Creating")

  expect_null(dasklyr:::dask_coiled_ready_id("missing", clusters))
})

test_that("dask_coiled_available is a single logical", {
  skip_on_cran()
  got <- dask_coiled_available()
  expect_type(got, "logical")
  expect_length(got, 1L)
  expect_false(is.na(got))
})

test_that("dask_coiled_clusters returns id/name/state", {
  skip_if_no_coiled_list()
  listed <- dask_coiled_clusters()
  expect_s3_class(listed, "tbl_df")
  expect_equal(names(listed), c("id", "name", "state"))
})

test_that("coiled attach reuses cluster_name and can copy_to", {
  target <- skip_if_no_coiled_attach()
  on.exit(dask_disconnect_all(), add = TRUE)

  expect_message(
    sc1 <- dask_connect(
      master = "coiled",
      cluster_name = target$name,
      n_workers = 99L,
      shutdown_on_close = FALSE
    ),
    "Attaching"
  )
  expect_true(dask_connection_is_open(sc1))
  expect_equal(sc1$backend, "coiled")
  expect_equal(sc1$cluster_name, target$name)
  expect_equal(sc1$coiled_id, target$coiled_id)

  expect_message(
    sc2 <- dask_connect(master = "coiled", cluster_name = target$name),
    "Re-using"
  )
  expect_true(identical(sc1, sc2))

  ping <- copy_to(sc1, data.frame(x = 1:3), "ping", overwrite = TRUE)
  expect_equal(dplyr::collect(ping)$x, 1:3)

  dask_disconnect(sc1)
  listed <- dask_coiled_clusters()
  expect_true(target$coiled_id %in% listed$id[listed$state == "ready"])
})

test_that("coiled create is opt-in and tears down", {
  name <- skip_if_no_coiled_create()
  on.exit(dask_disconnect_all(), add = TRUE)

  sc <- dask_connect(
    master = "coiled",
    cluster_name = name,
    n_workers = 1L,
    shutdown_on_close = TRUE
  )
  expect_true(dask_connection_is_open(sc))
  expect_equal(sc$cluster_name, name)
  expect_true(!is.null(sc$coiled_id))
  created_id <- sc$coiled_id

  dask_disconnect(sc)
  listed <- dask_coiled_clusters()
  row <- listed[listed$id == created_id, , drop = FALSE]
  if (nrow(row)) {
    expect_false(identical(row$state[[1L]], "ready"))
  }
})
