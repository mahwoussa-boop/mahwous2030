@echo off
color 0A
echo ==========================================================
echo       Mahwous 2030 Intelligence System - Auto Launcher
echo ==========================================================
echo.
echo Installing any missing libraries...
pip install -r requirements.txt >nul 2>nul
echo Starting Mahwous 2030 Application...
streamlit run app.py
pause
