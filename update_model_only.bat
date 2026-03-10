@echo off
echo ---------------------------------------
echo 1XW UPDATE - EXCEL DATA
echo ---------------------------------------

set EXCEL=1XW_TradeBlotter_Web.xlsx

python build_from_blotter_excel.py --file "%EXCEL%"

git add content/site_performance.json content/site_screener.json
git commit -m "Update Excel data"
git push

echo.
echo DONE
pause