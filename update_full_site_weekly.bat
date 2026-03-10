@echo off
echo ---------------------------------------
echo 1XW UPDATE - FULL WEEKLY PIPELINE
echo ---------------------------------------

set IBPORT=7497
set EXCEL=1XW_TradeBlotter_Web.xlsx

python engine_1xw_v1.py --port %IBPORT%
python build_from_blotter_excel.py --file "%EXCEL%"
python news_engine.py --days 7 --per_class 3
python weekly_compiler.py

git add content/site_screener.json content/site_performance.json content/news_digest.json content/site_weekly.json content/history/weeklies
git commit -m "Update weekly research package"
git push

echo.
echo DONE
pause