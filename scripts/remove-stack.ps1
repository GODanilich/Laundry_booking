$ErrorActionPreference = "Stop"

docker stack rm laundry
if ($LASTEXITCODE -ne 0) {
    throw "docker stack rm laundry failed with exit code $LASTEXITCODE"
}
Write-Host "Stack removal requested. Volumes are kept by Docker."
