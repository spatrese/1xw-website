@echo off
cd /d "%~dp0"

echo ================================
echo 1XW POSITION PREVIEW
echo ================================

echo.
echo Updating data from blotter...
python build_from_blotter_excel.py --file "1XW_TradeBlotter_Web.xlsx"

if %ERRORLEVEL% neq 0 (
    echo ERROR in build_from_blotter_excel.py
    pause
    exit /b
)

echo.
echo Generating preview...
python preview_position_alerts.py

if %ERRORLEVEL% neq 0 (
    echo ERROR in preview_position_alerts.py
    pause
    exit /b
)

echo.
echo ================================
echo PREVIEW READY
echo Check:
echo - content/position_alerts_preview.txt
echo ================================

pause