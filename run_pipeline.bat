@echo off

REM === Set project root for imports ===
set PYTHONPATH=D:\DS\projects\monitoreo_medios

REM === Activate conda environment and run pipeline ===
call D:\miniconda\Scripts\activate.bat monitoreo_medios
python D:\DS\projects\monitoreo_medios\main.py

REM === Keep window open on error ===
if %ERRORLEVEL% neq 0 (
    echo.
    echo Pipeline failed with error code %ERRORLEVEL%
    pause
)
