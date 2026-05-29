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

$services = @(
    "gateway-service",
    "identity-service",
    "machine-service",
    "schedule-service",
    "booking-service",
    "event-worker-service"
)

foreach ($service in $services) {
    Write-Host "Building laundry/${service}:local"
    Invoke-Docker build `
        -f "services/$service/Dockerfile" `
        -t "laundry/${service}:local" `
        .
}

Write-Host "All service images built."
