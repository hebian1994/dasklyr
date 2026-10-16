## Test environments

* local Windows 11, R 4.6.1, pandoc 3.12

## R CMD check results

0 errors | 0 warnings | 0 notes

## Update from 0.1.1 to 0.1.2

* Optional 'Coiled' backend: `dask_connect(master = "coiled",
  cluster_name = ...)`. Live Coiled tests are skipped unless
  `DASKLYR_COILED` is set; examples are in `\dontrun{}`.
* DESCRIPTION: quote 'Python' (and other software names) as requested
  in the 0.1.1 acceptance mail.
