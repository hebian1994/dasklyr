## Test environments

* local Windows 11, R 4.6.1, pandoc 3.12

## R CMD check results

0 errors | 0 warnings | 1 note

* New submission.

## Resubmission

Addressed incoming pretest notes for 0.1.0:

* DESCRIPTION spelling: 'Dask', 'dplyr', 'tibble', and 'reticulate' are
  quoted; "DataFrames" / "tibbles" / "SystemRequirements" reworded.
* README no longer contains a loopback http URL (dashboard described as
  host port 8787).
* `dask_available()` example is in \dontrun{}; the corresponding test
  uses skip_on_cran() so CRAN does not start Python.
