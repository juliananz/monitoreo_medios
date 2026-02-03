@echo off

REM === Set project root for imports ===
set PYTHONPATH=D:\DS\projects\monitoreo_medios

REM === Execute pipeline with Miniconda ===
"D:\miniconda\condabin\conda.bat" run -n monitoreo_medios python D:\DS\projects\monitoreo_medios\main.py

REM === End ===
