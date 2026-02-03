@echo off

REM === Run Streamlit dashboard ===

set PYTHONPATH=D:\DS\projects\monitoreo_medios

"D:\miniconda\condabin\conda.bat" run -n monitoreo_medios streamlit run D:\DS\projects\monitoreo_medios\app\app.py

REM === End ===
