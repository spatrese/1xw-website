@echo off
echo ---------------------------------------
echo 1XW UPDATE - TECHNICAL ANALYSIS
echo ---------------------------------------

set IBPORT=7497
set EXCEL=1XW_TradeBlotter_Web.xlsx

python engine_1xw_v1.py --port %IBPORT%

REM reinject model trades / positions from Excel
python build_from_blotter_excel.py --file "%EXCEL%"

REM rebuild weekly bundle
python weekly_compiler.py

git add content/site_screener.json content/site_performance.json content/site_weekly.json content/history/weeklies
git commit -m "Update technical analysis"
git push

echo.
echo DONE
pause