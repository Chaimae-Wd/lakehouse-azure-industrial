<#
.SYNOPSIS
    Arrête proprement la plateforme Industrial Lakehouse.

.DESCRIPTION
    Arrête :
    - les producteurs et traitements streaming ;
    - Kafka ;
    - PostgreSQL et pgAdmin ;
    - les services Apache Airflow.

    Les volumes Docker et les données sont conservés.
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

Clear-Host

Write-Host ""
Write-Host "========================================================" `
    -ForegroundColor Cyan
Write-Host "        ARRÊT DE LA PLATEFORME LAKEHOUSE" `
    -ForegroundColor Cyan
Write-Host "========================================================" `
    -ForegroundColor Cyan
Write-Host ""

Set-Location $ProjectRoot

Write-Host "[....] Arrêt des traitements streaming..." `
    -ForegroundColor Yellow

docker compose `
    --profile streaming `
    stop `
    sensor-producer `
    spark-streaming `
    spark-streaming-silver 2>$null

Write-Host "[ OK ] Traitements streaming arrêtés." `
    -ForegroundColor Green

Write-Host "[....] Arrêt de Kafka, PostgreSQL et pgAdmin..." `
    -ForegroundColor Yellow

docker compose `
    --profile streaming `
    stop `
    kafka `
    pgadmin `
    postgres

Write-Host "[ OK ] Services principaux arrêtés." `
    -ForegroundColor Green

Write-Host "[....] Arrêt d'Apache Airflow..." `
    -ForegroundColor Yellow

docker compose `
    -f $AirflowComposeFile `
    stop

Write-Host "[ OK ] Apache Airflow arrêté." `
    -ForegroundColor Green

Write-Host ""
Write-Host "========================================================" `
    -ForegroundColor Green
Write-Host "              PLATEFORME ARRÊTÉE" `
    -ForegroundColor Green
Write-Host "========================================================" `
    -ForegroundColor Green
Write-Host ""
Write-Host "Les volumes et les données ont été conservés."
Write-Host "Pour redémarrer :"
Write-Host ".\scripts\platform\start_platform.ps1" `
    -ForegroundColor Cyan
Write-Host ""