@echo off

REM === Run tests with pytest ===

set PYTHONPATH=D:\DS\projects\monitoreo_medios

"D:\miniconda\condabin\conda.bat" run -n monitoreo_medios pytest %*

REM === End ===
