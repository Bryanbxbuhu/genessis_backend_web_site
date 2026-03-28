@echo off
REM Sync all enabled cities with latest news
REM This script ensures all cities receive the latest news feeds
REM Run this daily or schedule it with Windows Task Scheduler

echo ============================================================
echo Syncing All Cities - Latest News Update
echo ============================================================
echo.

REM Activate virtual environment if it exists
if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
)

REM Sync all cities with reset for fresh data
echo Resetting runtime data and syncing all cities...
python scripts\reset_and_sync.py

echo.
echo ============================================================
echo Sync Complete! All cities now have the latest news.
echo ============================================================
echo.
echo To regenerate reports, run: python scripts\generate_all_reports.py
pause
