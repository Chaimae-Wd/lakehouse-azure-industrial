<#
.SYNOPSIS
    Affiche l'état de la plateforme Industrial Lakehouse.

.DESCRIPTION
    Vérifie :
    - Docker Desktop ;
    - PostgreSQL ;
    - pgAdmin ;
    - Kafka ;
    - Airflow API Server ;
    - Airflow Scheduler ;
    - la présence des couches Bronze, Silver et Gold.
#>

$ErrorActionPreference = "Continue"

[Console]::InputEncoding = [System.Text.UTF8Encoding]::new()
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$OutputEncoding = [System.Text.UTF8Encoding]::new()

$ScriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Resolve-Path (
    Join-Path $ScriptDirectory "..\.."
)

$AirflowComposeFile = Join-Path `
    $ProjectRoot `
    "airflow\docker-compose.airflow.yml"

function Write-Header {
    Clear-Host

    Write-Host ""
    Write-Host "============================================================" `
        -ForegroundColor Cyan
    Write-Host "          INDUSTRIAL LAKEHOUSE PLATFORM STATUS" `
        -ForegroundColor Cyan
    Write-Host "============================================================" `
        -ForegroundColor Cyan
    Write-Host ""
}

function Get-ContainerState {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ContainerName
    )

    $ContainerExists = docker ps -a `
        --filter "name=^/${ContainerName}$" `
        --format "{{.Names}}"

    if (-not $ContainerExists) {
        return "ABSENT"
    }

    $Running = docker inspect `
        --format "{{.State.Running}}" `
        $ContainerName 2>$null

    if ($Running -ne "true") {
        return "STOPPED"
    }

    $HealthStatus = docker inspect `
        --format `
        "{{if .State.Health}}{{.State.Health.Status}}{{else}}running{{end}}" `
        $ContainerName 2>$null

    if ($HealthStatus -eq "healthy") {
        return "HEALTHY"
    }

    if ($HealthStatus -eq "starting") {
        return "STARTING"
    }

    if ($HealthStatus -eq "unhealthy") {
        return "UNHEALTHY"
    }

    return "RUNNING"
}

function Write-ServiceStatus {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Service,

        [Parameter(Mandatory = $true)]
        [string]$Status
    )

    $Color = switch ($Status) {
        "HEALTHY" { "Green" }
        "RUNNING" { "Green" }
        "AVAILABLE" { "Green" }
        "STARTING" { "Yellow" }
        "EMPTY" { "Yellow" }
        "STOPPED" { "Red" }
        "UNHEALTHY" { "Red" }
        "ABSENT" { "DarkGray" }
        default { "White" }
    }

    Write-Host (
        "{0,-28} {1}" -f $Service, $Status
    ) -ForegroundColor $Color
}

function Test-DeltaLayer {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LayerPath
    )

    if (-not (Test-Path $LayerPath)) {
        return "ABSENT"
    }

    $DeltaLogs = Get-ChildItem `
        -Path $LayerPath `
        -Directory `
        -Recurse `
        -Filter "_delta_log" `
        -ErrorAction SilentlyContinue

    if ($DeltaLogs.Count -gt 0) {
        return "AVAILABLE"
    }

    return "EMPTY"
}

function Test-HttpEndpoint {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url
    )

    try {
        $Response = Invoke-WebRequest `
            -Uri $Url `
            -UseBasicParsing `
            -TimeoutSec 5

        if ($Response.StatusCode -ge 200 -and $Response.StatusCode -lt 500) {
            return "AVAILABLE"
        }

        return "UNHEALTHY"
    }
    catch {
        return "UNAVAILABLE"
    }
}

Write-Header
Set-Location $ProjectRoot

Write-Host "SERVICES DOCKER" -ForegroundColor Cyan
Write-Host "------------------------------------------------------------"

try {
    docker version *> $null

    if ($LASTEXITCODE -eq 0) {
        Write-ServiceStatus "Docker Desktop" "RUNNING"
    }
    else {
        Write-ServiceStatus "Docker Desktop" "STOPPED"
    }
}
catch {
    Write-ServiceStatus "Docker Desktop" "STOPPED"
}

Write-ServiceStatus `
    "PostgreSQL" `
(Get-ContainerState "ocp_postgres")

Write-ServiceStatus `
    "pgAdmin" `
(Get-ContainerState "ocp_pgadmin")

Write-ServiceStatus `
    "Kafka" `
(Get-ContainerState "industrial_kafka")

$AirflowApiContainer = (
    docker compose `
        -f $AirflowComposeFile `
        ps -q airflow-apiserver 2>$null
).Trim()

if ($AirflowApiContainer) {
    $AirflowApiName = docker inspect `
        --format "{{.Name}}" `
        $AirflowApiContainer

    $AirflowApiName = $AirflowApiName.TrimStart("/")

    Write-ServiceStatus `
        "Airflow API Server" `
    (Get-ContainerState $AirflowApiName)
}
else {
    Write-ServiceStatus "Airflow API Server" "ABSENT"
}

$AirflowSchedulerContainer = (
    docker compose `
        -f $AirflowComposeFile `
        ps -q airflow-scheduler 2>$null
).Trim()

if ($AirflowSchedulerContainer) {
    $AirflowSchedulerName = docker inspect `
        --format "{{.Name}}" `
        $AirflowSchedulerContainer

    $AirflowSchedulerName = $AirflowSchedulerName.TrimStart("/")

    Write-ServiceStatus `
        "Airflow Scheduler" `
    (Get-ContainerState $AirflowSchedulerName)
}
else {
    Write-ServiceStatus "Airflow Scheduler" "ABSENT"
}

Write-Host ""
Write-Host "INTERFACES WEB" -ForegroundColor Cyan
Write-Host "------------------------------------------------------------"

Write-ServiceStatus `
    "Airflow UI (127.0.0.1:8080)" `
(Test-HttpEndpoint "http://127.0.0.1:8080/")

Write-ServiceStatus `
    "pgAdmin (127.0.0.1:5050)" `
(Test-HttpEndpoint "http://127.0.0.1:5050/")

Write-Host ""
Write-Host "COUCHES LAKEHOUSE" -ForegroundColor Cyan
Write-Host "------------------------------------------------------------"

Write-ServiceStatus `
    "Bronze Batch" `
(Test-DeltaLayer (Join-Path $ProjectRoot "data\bronze"))

Write-ServiceStatus `
    "Silver Batch" `
(Test-DeltaLayer (Join-Path $ProjectRoot "data\silver"))

Write-ServiceStatus `
    "Gold Batch" `
(Test-DeltaLayer (Join-Path $ProjectRoot "data\gold"))

Write-ServiceStatus `
    "Bronze Streaming" `
(Test-DeltaLayer (
        Join-Path $ProjectRoot "data\bronze_streaming"
    ))

Write-ServiceStatus `
    "Silver Streaming" `
(Test-DeltaLayer (
        Join-Path $ProjectRoot "data\silver_streaming"
    ))

Write-Host ""
Write-Host "ACCÈS À LA PLATEFORME" -ForegroundColor Cyan
Write-Host "------------------------------------------------------------"
Write-Host "Airflow    : http://127.0.0.1:8080/"
Write-Host "pgAdmin    : http://127.0.0.1:5050/"
Write-Host "PostgreSQL : localhost:5432"
Write-Host "Kafka      : localhost:29092"
