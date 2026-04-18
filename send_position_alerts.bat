@echo off
cd /d "%~dp0"

echo ================================
echo 1XW POSITION ALERTS
echo ================================

python send_position_alerts.py

if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR in send_position_alerts.py
    pause
    exit /b
)

echo.
echo ================================
echo DONE
echo ================================
pause