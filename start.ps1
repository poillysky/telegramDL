# 本地启动。默认不热重载（稳定）；开发改后端时加 -Reload
param(
    [switch]$Reload
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$uvicorn = Join-Path $PSScriptRoot ".venv\Scripts\uvicorn.exe"
if (-not (Test-Path $uvicorn)) {
    Write-Error "未找到 .venv，请先: python -m venv .venv; .\.venv\Scripts\pip install -r requirements.txt"
}

$listeners = Get-NetTCPConnection -LocalPort 9345 -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique
foreach ($procId in $listeners) {
    if ($procId -and $procId -ne 0) {
        Write-Host "Stopping old process on :9345 PID=$procId"
        Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
    }
}
# Clear wedged python/uvicorn that still mention this app
Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match 'uvicorn.*app\.main|telegram' } |
    ForEach-Object {
        Write-Host "Force-stop python PID=$($_.ProcessId)"
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }

Start-Sleep -Milliseconds 500

if ($Reload) {
    Write-Host "Starting WITH auto-reload on http://127.0.0.1:9345"
    Write-Host "(改 .py 会重启；下载/联调中请用 .\start.ps1 不要 -Reload)"
    & $uvicorn app.main:app `
        --host 0.0.0.0 `
        --port 9345 `
        --reload `
        --reload-dir app `
        --reload-include "*.py" `
        --reload-exclude "*__pycache__*" `
        --reload-exclude "*_tmp_*" `
        --timeout-graceful-shutdown 2 `
        --timeout-keep-alive 2
} else {
    Write-Host "Starting (stable, no reload) on http://127.0.0.1:9345"
    & $uvicorn app.main:app `
        --host 0.0.0.0 `
        --port 9345 `
        --timeout-keep-alive 5
}
