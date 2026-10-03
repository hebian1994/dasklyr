# dasklyr 0.1.2

* `dask_connect(master = "coiled", cluster_name = ...)` opens or attaches
  a Coiled cluster through Python `coiled.Cluster`. `dask_coiled_clusters()`
  lists account clusters. Tests: default never creates a cluster;
  `DASKLYR_COILED=true` attaches a ready `DASKLYR_COILED_NAME`;
  `DASKLYR_COILED=create` is required to start a new id.

# dasklyr 0.1.1

* CRAN incoming pretest: quote software names in DESCRIPTION, drop the
  loopback dashboard link from README, and do not start Python from
  examples or CRAN tests via `dask_available()`.

# dasklyr 0.1.0

* First public release: connect to local or remote Dask, `copy_to()` /
  `tbl()`, dplyr verbs, Parquet and CSV, `ddf_*` table helpers, and
  `dask_invoke()`.
