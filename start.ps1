# 本地开发启动：改 app/ 下代码会自动重启
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$uvicorn = Join-Path $PSScriptRoot ".venv\Scripts\uvicorn.exe"
if (-not (Test-Path $uvicorn)) {
    Write-Error "未找到 .venv，请先: python -m venv .venv; .\.venv\Scripts\pip install -r requirements.txt"
}

$listeners = Get-NetTCPConnection -LocalPort 9345 -State Listen -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique
foreach ($procId in $listeners) {
    Write-Host "Stopping old process on :9345 PID=$procId"
    Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
}

Write-Host "Starting with auto-reload on http://127.0.0.1:9345"
& $uvicorn app.main:app --host 0.0.0.0 --port 9345 --reload --reload-dir app
