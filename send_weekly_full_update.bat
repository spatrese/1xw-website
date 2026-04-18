@echo off
cd /d "%~dp0"

echo ================================
echo 1XW WEEKLY FULL UPDATE
echo ================================

echo.
echo Running weekly report...
python send_weekly_report.py

if %ERRORLEVEL% neq 0 (
    echo ERROR in send_weekly_report.py
    pause
    exit /b
)

echo.
echo Running performance update...
python send_performance_update.py

if %ERRORLEVEL% neq 0 (
    echo ERROR in send_performance_update.py
    pause
    exit /b
)

echo.
echo ================================
echo DONE
echo ================================
pause