## Test environments

* local Windows 11, R 4.6.1, pandoc 3.12

## R CMD check results

0 errors | 0 warnings | 1 note

* New submission.

Python with the 'dask' package is a SystemRequirement. Examples that
start a Dask client are wrapped in \dontrun{}. Tests that need Dask
call skip_on_cran() and skip when dask is not installed, so CRAN
machines without Python/Dask still pass.
