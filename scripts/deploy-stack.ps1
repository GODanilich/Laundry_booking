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

$swarmState = docker info --format "{{.Swarm.LocalNodeState}}" 2>$null
if ($swarmState -ne "active") {
    Write-Host "Initializing Docker Swarm..."
    Invoke-Docker swarm init
}

Write-Host "Deploying laundry stack..."
Invoke-Docker stack deploy --prune -c deployments/docker-stack.yml laundry

Write-Host "Stack deployment requested. Check status with:"
Write-Host "docker stack services laundry"
