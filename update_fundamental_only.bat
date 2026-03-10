@echo off
echo ---------------------------------------
echo 1XW UPDATE - NEWS / EVENTS
echo ---------------------------------------

python news_engine.py --days 7 --per_class 3
python weekly_compiler.py

git add content/news_digest.json content/site_weekly.json content/history/weeklies
git commit -m "Update news and weekly research"
git push

echo.
echo DONE
pause