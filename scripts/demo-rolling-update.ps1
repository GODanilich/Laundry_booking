$ErrorActionPreference = "Stop"

function Invoke-Docker {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]] $Arguments
    )

    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

Write-Host "Building booking-service v2 image..."
Invoke-Docker build `
    -f "services/booking-service/Dockerfile" `
    -t "laundry/booking-service:v2" `
    .

Write-Host "Updating only booking-service with start-first rolling update..."
Invoke-Docker service update `
    --image "laundry/booking-service:v2" `
    --env-rm SERVICE_VERSION `
    --env-add SERVICE_VERSION=v2 `
    --update-order start-first `
    --update-parallelism 1 `
    laundry_booking-service

Write-Host "Watch update status with:"
Write-Host "docker service ps laundry_booking-service"
