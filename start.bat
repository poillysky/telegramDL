@echo off
cd /d "%~dp0"

if not exist ".venv\Scripts\uvicorn.exe" (
  echo [ERROR] 未找到 .venv，请先: python -m venv .venv ^&^& .venv\Scripts\pip install -r requirements.txt
  exit /b 1
)

REM 释放 9345 端口上的旧进程，避免改代码后仍跑旧实例
for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":9345" ^| findstr "LISTENING"') do (
  echo Stopping old process on :9345 PID=%%p
  taskkill /F /PID %%p >nul 2>&1
)

echo Starting with auto-reload on http://127.0.0.1:9345
".venv\Scripts\uvicorn.exe" app.main:app --host 0.0.0.0 --port 9345 --reload --reload-dir app
