# dasklyr

[![CRAN status](https://www.r-pkg.org/badges/version/dasklyr)](https://cran.r-project.org/package=dasklyr)
[![CRAN downloads](https://cranlogs.r-pkg.org/badges/grand-total/dasklyr)](https://cran.r-project.org/package=dasklyr)

R front-end for [Dask](https://www.dask.org/), aimed at the same user-facing
shape as [sparklyr](https://spark.posit.co/) (`connect` / `copy_to` / dplyr /
read-write / collect). R talks to the Python Dask client through
[reticulate](https://rstudio.github.io/reticulate/). Workers stay Python.

On CRAN: `install.packages("dasklyr")`.

## Table of Contents

- [Installation](#installation)
- [Connecting to Dask](#connecting-to-dask)
- [Using dplyr](#using-dplyr)
- [Reading and Writing Data](#reading-and-writing-data)
- [Table Utilities](#table-utilities)
- [Connection Utilities](#connection-utilities)
- [Docker cluster](#docker-cluster)
- [Examples](#examples)

## Installation

```r
install.packages("dasklyr")
```

Development version from this repository (`Rscript` on `PATH`):

```r
install.packages(".", repos = NULL, type = "source")
```

Install Python Dask at the **same major release** as the cluster you connect
to:

```
pip install "dask[dataframe,distributed]" pyarrow pandas
```

Point reticulate at that interpreter with **environment variables**, not
hardcoded paths:

| Variable | Purpose |
| --- | --- |
| `DASKLYR_PYTHON` | Python executable with dask (read by this package) |
| `RETICULATE_PYTHON` | Same, via reticulate |
| `DASK_SCHEDULER` | Cluster address, for example `tcp://` host port `8786` |

## Connecting to Dask

Connect to a local client or a remote scheduler. `dask_connect()` with no
arguments is always local; pass `master` for a cluster (or
`dask_connect(Sys.getenv("DASK_SCHEDULER"))`).

```r
library(dasklyr)
sc <- dask_connect(master = "local")
# sc <- dask_connect(master = Sys.getenv("DASK_SCHEDULER"))
```

The returned connection (`sc`) is a dplyr source. The same `master` +
`app_name` pair reuses an open local / `tcp://` connection.

For [Coiled](https://docs.coiled.io/) (`pip install coiled` and
`coiled login` in the same Python as `DASKLYR_PYTHON`):

```r
sc <- dask_connect(
  master = "coiled",
  cluster_name = "demo-keep",
  n_workers = 1L,
  shutdown_on_close = FALSE
)
dask_coiled_clusters()
```

`cluster_name` is the Coiled cluster identity. A stopped cluster of the
same name starts a new id. `shutdown_on_close = FALSE` leaves the VMs
after `dask_disconnect()`.

## Using dplyr

Copy data frames from R into Dask, then use dplyr verbs. Computation stays
lazy until `collect()`.

```r
library(dplyr)

students <- data.frame(
  name = c("Ann", "Bob", "Cara", "Dan"),
  class = c("a", "a", "b", "b"),
  math = c(92L, 78L, 88L, 95L),
  english = c(85L, 90L, 76L, 88L)
)
classes <- data.frame(
  class = c("a", "b"),
  teacher = c("Chen", "Liu")
)

st <- copy_to(sc, students, overwrite = TRUE)
cl <- copy_to(sc, classes, overwrite = TRUE)
src_tbls(sc)
```

Filter, mutate, aggregate:

```r
st %>%
  filter(math >= 90) %>%
  mutate(total = math + english) %>%
  group_by(class) %>%
  summarise(n = n(), avg_math = mean(math), .groups = "drop") %>%
  collect()
```

Joins use the same `by` argument as dplyr, including named `c("a" = "b")`:

```r
st %>%
  left_join(cl, by = "class") %>%
  select(name, class, teacher, math) %>%
  collect()
```

Supported verbs: `select`, `rename`, `filter` (comparisons, `%in%`, `is.na`),
`mutate` (columns, literals, `+ - * /`), `arrange` / `desc()`, `group_by` /
`ungroup`, `summarise` (`n()`, `n_distinct`, `mean` / `sum` / `min` / `max`),
`count`, `distinct`, `slice_head`, `left_join` / `inner_join` / `semi_join`,
`collect()`.

`head()` is the first rows of the **first partition**. Use `slice_head()`
when you need a global first-n.

Window functions, `across()`, and nested `case_when` are not implemented.

```
Rscript examples/local_demo.R
```

## Reading and Writing Data

```r
dask_write_parquet(st, "students.parquet", overwrite = TRUE)
pq <- dask_read_parquet(sc, "students_pq", "students.parquet")

dask_write_csv(st, "students.csv")
csv <- dask_read_csv(sc, "students_csv", "students.csv")
```

`path` must be visible to **workers**. A laptop `tempfile()` is fine for
`master = "local"`. On a Docker cluster it is not your Windows temp
directory; use a shared volume or object store.

## Table Utilities

Counterparts of sparklyr's `sdf_*` helpers, named `ddf_*`:

```r
ddf_npartitions(st)
ddf_schema(st)
ddf_nrow(st)
ddf_repartition(st, npartitions = 4)
ddf_persist(st)
```

`dask_dataframe(st)` returns the Python Dask DataFrame.
`dask_invoke(st, "repartition", npartitions = 2L)` calls an unwrapped
Python method (sparklyr's `invoke()`).

## Connection Utilities

```r
dask_web(sc)              # dashboard URL (opens a browser when interactive)
dask_connection_find()
dask_coiled_clusters()    # Coiled account clusters (optional)
dask_disconnect(sc)
dask_disconnect_all()
```

## Docker cluster

Image tag is pinned to **dask 2026.8.0**. Scheduler port `8786`, dashboard
port `8787` on the scheduler host (Docker publishes these on the local
machine).

The official image is a conda stack (often Python 3.10 / pandas 2.x). Your
client Python should use the same **dask** release; other libraries may
still warn `VersionMismatchWarning`. Computations can still run. For fewer
warnings, match Python/pandas as well, or install `lz4` on the client.

```
docker compose up -d --wait --scale worker=2
Rscript examples/cluster_demo.R
docker compose down
```

Or `examples/run_cluster_demo.ps1` (`docker` and `Rscript` on `PATH`).
Default scheduler: set `DASK_SCHEDULER` to the scheduler `tcp://` address
(Docker Compose uses host port `8786`).

## Examples

Run from the **package root**. `examples/` is not in the CRAN tarball.

| Script | Backend | Notes |
| --- | --- | --- |
| `examples/local_demo.R` | `master = "local"` | dplyr, `ddf_*`, parquet/csv (`tempfile` is fine) |
| `examples/cluster_demo.R` | `DASK_SCHEDULER` / Docker | no laptop tempfile writes |
| `examples/coiled_demo.R` | `master = "coiled"` | lists clusters; attaches `DASKLYR_COILED_NAME` if **ready**; `DASKLYR_COILED=create` is required to start a new id |

```
Rscript examples/local_demo.R
Rscript examples/cluster_demo.R
Rscript examples/coiled_demo.R
```

PowerShell wrappers: `examples/run_local_demo.ps1`, `run_cluster_demo.ps1`,
`run_coiled_demo.ps1` (`Rscript` on `PATH`; Coiled wrapper requires
`DASKLYR_COILED_NAME`).

## Not in scope

MLlib-style `ml_*`, running R on workers (`spark_apply`), Spark streaming,
JDBC, and a SQL `dbGetQuery` surface. Those have no Dask counterpart worth
shipping as empty wrappers.
