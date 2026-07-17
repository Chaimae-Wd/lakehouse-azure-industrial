param(
    [Parameter(Position = 0)]
    [ValidateSet("start", "status", "stop")]
    [string]$Action = "status"
)

$ScriptsRoot = Join-Path $PSScriptRoot "scripts\platform"

switch ($Action) {
    "start" {
        & (Join-Path $ScriptsRoot "start_platform.ps1")
    }

    "status" {
        & (Join-Path $ScriptsRoot "status_platform.ps1")
    }

    "stop" {
        & (Join-Path $ScriptsRoot "stop_platform.ps1")
    }
}