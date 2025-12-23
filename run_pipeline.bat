@echo off

REM === Activar Anaconda ===
call "C:\Users\julia\anaconda3\Scripts\activate.bat"

REM === Activar entorno del proyecto ===
call conda activate monitoreo_medios

REM === Ir al directorio del proyecto ===
cd /d D:\DS\projects\monitoreo_medios

REM === Ejecutar pipeline ===
python main.py

REM === Fin ===
