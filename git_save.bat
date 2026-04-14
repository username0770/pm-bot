@echo off
cd /d "E:\project\bot polymarket\arb_bot"
git add -A
git status --short
echo.
set /p msg="Commit message (Enter for default): "
if "%msg%"=="" set msg=update: auto-save %date% %time%
git commit -m "%msg%"
git push origin main
echo.
echo Done! Pushed to GitHub.
pause
