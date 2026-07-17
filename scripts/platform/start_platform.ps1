<#
.SYNOPSIS
    Démarre la plateforme Industrial Lakehouse locale.

.DESCRIPTION
    Ce script :
    1. vérifie que Docker Desktop est disponible ;
    2. démarre PostgreSQL et pgAdmin ;
    3. démarre Kafka en mode KRaft ;
    4. démarre Apache Airflow ;
    5. attend que les services deviennent opérationnels ;
    6. affiche l'état global de la plateforme ;
    7. ouvre les interfaces Web principales.

.NOTES
    Projet : lakehouse-azure-industrial
    Auteure : Chaimae El Widadi
#>

$ErrorActionPreference = "Stop"

# Utiliser UTF-8 pour afficher correctement les accents.
[Console]::InputEncoding = [System.Text.UTF8Encoding]::new()
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$OutputEncoding = [System.Text.UTF8Encoding]::new()

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

$ScriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Resolve-Path (
    Join-Path $ScriptDirectory "..\.."
)

$MainComposeFile = Join-Path $ProjectRoot "docker-compose.yml"
$AirflowComposeFile = Join-Path `
    $ProjectRoot `
    "airflow\docker-compose.airflow.yml"

$AirflowUrl = "http://127.0.0.1:8080/"
$PgAdminUrl = "http://127.0.0.1:5050/"

# ------------------------------------------------------------
# Fonctions d'affichage
# ------------------------------------------------------------

function Write-PlatformHeader {
    Clear-Host

    Write-Host ""
    Write-Host "========================================================" `
        -ForegroundColor Cyan
    Write-Host "        INDUSTRIAL LAKEHOUSE PLATFORM" `
        -ForegroundColor Cyan
    Write-Host "========================================================" `
        -ForegroundColor Cyan
    Write-Host ""
}

function Write-Step {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    Write-Host "[....] $Message" -ForegroundColor Yellow
}

function Write-Success {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    Write-Host "[ OK ] $Message" -ForegroundColor Green
}

function Write-Failure {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    Write-Host "[ERREUR] $Message" -ForegroundColor Red
}

# ------------------------------------------------------------
# Vérifications
# ------------------------------------------------------------

function Test-DockerAvailability {
    Write-Step "Vérification de Docker Desktop..."

    docker version *> $null

    if ($LASTEXITCODE -ne 0) {
        throw (
            "Docker Desktop n'est pas disponible. " +
            "Ouvre Docker Desktop puis relance le script."
        )
    }

    Write-Success "Docker Desktop est disponible."
}

function Test-ProjectFiles {
    Write-Step "Vérification des fichiers Docker Compose..."

    if (-not (Test-Path $MainComposeFile)) {
        throw "Fichier introuvable : $MainComposeFile"
    }

    if (-not (Test-Path $AirflowComposeFile)) {
        throw "Fichier introuvable : $AirflowComposeFile"
    }

    Write-Success "Les fichiers Docker Compose sont présents."
}

function Wait-ComposeService {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$ComposeArguments,

        [Parameter(Mandatory = $true)]
        [string]$ServiceName,

        [int]$TimeoutSeconds = 180
    )

    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $Deadline) {
        $ContainerId = (
            docker compose @ComposeArguments ps -q $ServiceName
        ).Trim()

        if ($ContainerId) {
            $ContainerStatus = (
                docker inspect `
                    --format `
                    "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" `
                    $ContainerId
            ).Trim()

            if (
                $ContainerStatus -eq "healthy" -or
                $ContainerStatus -eq "running"
            ) {
                return $ContainerStatus
            }
        }

        Start-Sleep -Seconds 5
    }

    throw (
        "Le service '$ServiceName' n'est pas devenu " +
        "opérationnel après $TimeoutSeconds secondes."
    )
}

# ------------------------------------------------------------
# Démarrage des services
# ------------------------------------------------------------

function Start-CoreServices {
    Write-Step "Démarrage de PostgreSQL, pgAdmin et Kafka..."

    docker compose `
        --profile streaming `
        up -d `
        postgres `
        pgadmin `
        kafka

    if ($LASTEXITCODE -ne 0) {
        throw "Échec du démarrage des services principaux."
    }

    Write-Success "Les conteneurs principaux ont été lancés."
}

function Start-AirflowServices {
    Write-Step "Démarrage d'Apache Airflow..."

    docker compose `
        -f $AirflowComposeFile `
        up -d

    if ($LASTEXITCODE -ne 0) {
        throw "Échec du démarrage d'Apache Airflow."
    }

    Write-Success "Les conteneurs Airflow ont été lancés."
}

function Wait-ForPlatform {
    Write-Step "Attente de PostgreSQL..."

    Wait-ComposeService `
        -ComposeArguments @("--profile", "streaming") `
        -ServiceName "postgres" `
        -TimeoutSeconds 120 | Out-Null

    Write-Success "PostgreSQL est opérationnel."

    Write-Step "Attente de Kafka..."

    Wait-ComposeService `
        -ComposeArguments @("--profile", "streaming") `
        -ServiceName "kafka" `
        -TimeoutSeconds 180 | Out-Null

    Write-Success "Kafka est opérationnel."

    Write-Step "Attente du serveur Airflow..."

    Wait-ComposeService `
        -ComposeArguments @(
        "-f",
        $AirflowComposeFile
    ) `
        -ServiceName "airflow-apiserver" `
        -TimeoutSeconds 240 | Out-Null

    Write-Success "Airflow API Server est opérationnel."

    Write-Step "Attente du scheduler Airflow..."

    Wait-ComposeService `
        -ComposeArguments @(
        "-f",
        $AirflowComposeFile
    ) `
        -ServiceName "airflow-scheduler" `
        -TimeoutSeconds 240 | Out-Null

    Write-Success "Airflow Scheduler est opérationnel."
}

# ------------------------------------------------------------
# État final
# ------------------------------------------------------------

function Show-PlatformStatus {
    Write-Host ""
    Write-Host "---------------- SERVICES PRINCIPAUX ----------------" `
        -ForegroundColor Cyan

    docker compose `
        --profile streaming `
        ps `
        postgres `
        pgadmin `
        kafka

    Write-Host ""
    Write-Host "---------------- SERVICES AIRFLOW -------------------" `
        -ForegroundColor Cyan

    docker compose `
        -f $AirflowComposeFile `
        ps

    Write-Host ""
}

function Open-PlatformInterfaces {
    Write-Step "Ouverture des interfaces Web..."

    Start-Process $AirflowUrl
    Start-Process $PgAdminUrl

    Write-Success "Les interfaces Web ont été ouvertes."
}

function Show-FinalMessage {
    Write-Host ""
    Write-Host "========================================================" `
        -ForegroundColor Green
    Write-Host "               PLATEFORME PRÊTE" `
        -ForegroundColor Green
    Write-Host "========================================================" `
        -ForegroundColor Green

    Write-Host ""
    Write-Host "Airflow     : $AirflowUrl"
    Write-Host "pgAdmin     : $PgAdminUrl"
    Write-Host "PostgreSQL  : localhost:5432"
    Write-Host "Kafka       : localhost:29092"
    Write-Host ""

    Write-Host "Prochaine action :" -ForegroundColor Cyan
    Write-Host (
        "1. Ouvrir Airflow et déclencher le DAG " +
        "'industrial_lakehouse_batch_pipeline'."
    )
    Write-Host "2. Vérifier que toutes les tâches deviennent vertes."
    Write-Host "3. Actualiser ensuite le rapport Power BI."
    Write-Host ""
}

# ------------------------------------------------------------
# Programme principal
# ------------------------------------------------------------

try {
    Write-PlatformHeader

    Set-Location $ProjectRoot

    Test-DockerAvailability
    Test-ProjectFiles

    Start-CoreServices
    Start-AirflowServices

    Wait-ForPlatform
    Show-PlatformStatus
    Open-PlatformInterfaces
    Show-FinalMessage
}
catch {
    Write-Host ""
    Write-Failure $_.Exception.Message

    Write-Host ""
    Write-Host "Consulte les logs avec :" -ForegroundColor Yellow
    Write-Host (
        "docker compose --profile streaming logs --tail 100"
    )
    Write-Host (
        "docker compose -f " +
        "airflow/docker-compose.airflow.yml logs --tail 100"
    )

    exit 1
}
finally {
    Set-Location $ProjectRoot
}