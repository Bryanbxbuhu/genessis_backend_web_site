@echo off
REM Complete workflow: Sync all cities AND regenerate all reports
REM Use this for a complete refresh of all data and reports

echo ============================================================
echo Complete Refresh: Sync + Generate All Reports
echo ============================================================
echo.

REM Activate virtual environment if it exists
if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
)

echo [Step 1/2] Syncing all cities with latest news...
python scripts\sync_supabase.py --all --skip-context --force

echo.
echo [Step 2/2] Regenerating all reports with latest data...
python run_report.py --all --skip-pdf

echo.
echo ============================================================
echo Complete! All cities synced and reports regenerated.
echo Check the reports\ folder for updated PDFs.
echo ============================================================
pause
