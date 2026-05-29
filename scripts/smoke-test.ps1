$ErrorActionPreference = "Stop"

$BaseUrl = if ($env:BASE_URL) { $env:BASE_URL } else { "http://localhost:8080" }
$Today = (Get-Date).ToString("yyyy-MM-dd")
$UserEmail = "user-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())@example.com"

function Invoke-Api {
    param(
        [string] $Method,
        [string] $Path,
        [object] $Body = $null,
        [hashtable] $Headers = @{}
    )

    $params = @{
        Method = $Method
        Uri = "$BaseUrl$Path"
        Headers = $Headers
    }

    if ($null -ne $Body) {
        $params.Body = ($Body | ConvertTo-Json -Depth 10)
        $params.ContentType = "application/json"
    }

    Invoke-RestMethod @params
}

Write-Host "Checking gateway health..."
Invoke-Api -Method GET -Path "/health" | ConvertTo-Json -Depth 10

Write-Host "Logging in as admin..."
$adminTokens = Invoke-Api -Method POST -Path "/api/v1/auth/login" -Body @{
    email = "admin@example.com"
    password = "Admin123"
}
$adminHeaders = @{ Authorization = "Bearer $($adminTokens.access_token)" }

Write-Host "Loading machines..."
$machines = Invoke-Api -Method GET -Path "/api/v1/machines"
if ($machines.items.Count -eq 0) {
    Write-Host "No machines found, creating demo machine..."
    $machine = Invoke-Api -Method POST -Path "/api/v1/admin/machines" -Headers $adminHeaders -Body @{
        name = "Machine 1"
        location = "Dormitory 1"
        capacity_kg = 6
    }
} else {
    $machine = $machines.items[0]
}

Write-Host "Using machine $($machine.id)"

Write-Host "Generating slots for $Today..."
Invoke-Api -Method POST -Path "/api/v1/admin/slots/generate" -Headers $adminHeaders -Body @{
    machine_id = $machine.id
    date = $Today
    slot_duration_minutes = 60
    start_time = "08:00"
    end_time = "12:00"
} | ConvertTo-Json -Depth 10

Write-Host "Registering test user $UserEmail..."
Invoke-Api -Method POST -Path "/api/v1/auth/register" -Body @{
    email = $UserEmail
    password = "Password123"
    full_name = "Smoke Test User"
} | ConvertTo-Json -Depth 10

Write-Host "Logging in as test user..."
$userTokens = Invoke-Api -Method POST -Path "/api/v1/auth/login" -Body @{
    email = $UserEmail
    password = "Password123"
}
$userHeaders = @{ Authorization = "Bearer $($userTokens.access_token)" }

Write-Host "Loading available slots..."
$slots = Invoke-Api -Method GET -Path "/api/v1/slots?machine_id=$($machine.id)&date=$Today"
if ($slots.items.Count -eq 0) {
    throw "No available slots for smoke test"
}
$slot = $slots.items[0]

Write-Host "Creating booking for slot $($slot.id)..."
$booking = Invoke-Api -Method POST -Path "/api/v1/bookings" -Headers $userHeaders -Body @{
    machine_id = $machine.id
    slot_id = $slot.id
}
$booking | ConvertTo-Json -Depth 10

Write-Host "Waiting for mock payment..."
Start-Sleep -Seconds 6

Write-Host "User bookings:"
Invoke-Api -Method GET -Path "/api/v1/bookings/my" -Headers $userHeaders | ConvertTo-Json -Depth 10

Write-Host "Analytics summary:"
Invoke-Api -Method GET -Path "/api/v1/admin/analytics/summary" -Headers $adminHeaders | ConvertTo-Json -Depth 10

Write-Host "Recent audit records:"
Invoke-Api -Method GET -Path "/api/v1/admin/audit?limit=10" -Headers $adminHeaders | ConvertTo-Json -Depth 10

Write-Host "Smoke test completed."

