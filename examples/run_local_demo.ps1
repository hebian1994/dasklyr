# Run examples/local_demo.R from the package root.
# Needs: Rscript on PATH, dasklyr installed, Python with dask.
# Optional: DASKLYR_PYTHON

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$rscript = Get-Command Rscript -ErrorAction SilentlyContinue
if (-not $rscript) {
  throw "Rscript not found on PATH. Add R's bin directory to PATH, then retry."
}

& $rscript.Source --encoding=UTF-8 (Join-Path $Root "examples\local_demo.R")
if ($LASTEXITCODE -ne 0) {
  throw "local_demo.R failed with exit code $LASTEXITCODE"
}
