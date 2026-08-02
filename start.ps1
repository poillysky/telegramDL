# 本地开发启动：改 Python 会自动重启；静态 web 文件不触发重载（避免下载中卡死）
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
# Also clear wedged reloaders that still hold the port in weird states
Get-NetTCPConnection -LocalPort 9345 -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique |
    ForEach-Object {
        Write-Host "Force-stop PID=$_ on :9345"
        Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue
    }

Start-Sleep -Milliseconds 400

Write-Host "Starting with auto-reload on http://127.0.0.1:9345"
& $uvicorn app.main:app `
    --host 0.0.0.0 `
    --port 9345 `
    --reload `
    --reload-dir app `
    --reload-include "*.py" `
    --timeout-graceful-shutdown 3 `
    --timeout-keep-alive 5
