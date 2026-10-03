# Attach a ready Coiled cluster and run examples/coiled_demo.R.
# Does not create a cluster. Set DASKLYR_COILED=create yourself if you
# intend to pay for a new id.
#
# Needs: Rscript on PATH, dasklyr installed, Python with coiled + dask,
#        coiled login / DASK_COILED__TOKEN
# Required: DASKLYR_COILED_NAME
# Optional: DASKLYR_PYTHON

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (-not $env:DASKLYR_COILED_NAME) {
  throw "Set DASKLYR_COILED_NAME to a ready Coiled cluster. This script will not invent a name."
}

$rscript = Get-Command Rscript -ErrorAction SilentlyContinue
if (-not $rscript) {
  throw "Rscript not found on PATH. Add R's bin directory to PATH, then retry."
}

& $rscript.Source --encoding=UTF-8 (Join-Path $Root "examples\coiled_demo.R")
if ($LASTEXITCODE -ne 0) {
  throw "coiled_demo.R failed with exit code $LASTEXITCODE"
}
