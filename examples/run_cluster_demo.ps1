# Start the Docker Dask cluster and run examples/cluster_demo.R
#
# Needs: docker, Rscript on PATH, dasklyr installed, Python with matching dask.
# Optional: DASKLYR_PYTHON, DASK_SCHEDULER, DASK_WORKERS (default 2)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$workers = 2
if ($env:DASK_WORKERS) { $workers = [int]$env:DASK_WORKERS }

docker compose up -d --wait --scale "worker=$workers"

if (-not $env:DASK_SCHEDULER) {
  $env:DASK_SCHEDULER = "tcp://127.0.0.1:8786"
}

$rscript = Get-Command Rscript -ErrorAction SilentlyContinue
if (-not $rscript) {
  throw "Rscript not found on PATH. Add R's bin directory to PATH, then retry."
}

Write-Host "dashboard: http://127.0.0.1:8787"
& $rscript.Source --encoding=UTF-8 (Join-Path $Root "examples\cluster_demo.R")
if ($LASTEXITCODE -ne 0) {
  throw "cluster_demo.R failed with exit code $LASTEXITCODE"
}
