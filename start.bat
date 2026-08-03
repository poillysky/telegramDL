@echo off
cd /d "%~dp0"

if not exist ".venv\Scripts\uvicorn.exe" (
  echo [ERROR] 未找到 .venv，请先: python -m venv .venv ^&^& .venv\Scripts\pip install -r requirements.txt
  exit /b 1
)

REM 释放 9345 端口上的旧进程
for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":9345" ^| findstr "LISTENING"') do (
  echo Stopping old process on :9345 PID=%%p
  taskkill /F /PID %%p >nul 2>&1
)

echo Starting (stable, no reload) on http://127.0.0.1:9345
echo 开发热重载请用: powershell -File .\start.ps1 -Reload
".venv\Scripts\uvicorn.exe" app.main:app --host 0.0.0.0 --port 9345 --timeout-keep-alive 5
