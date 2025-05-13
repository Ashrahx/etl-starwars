@echo off
SET PYTHON_PATH="C:\Python39\python.exe"
SET SCRIPT_PATH="%~dp0src\etl_process.py"

SCHTASKS /CREATE /SC DAILY /TN "PokemonETL" /TR "%PYTHON_PATH% %SCRIPT_PATH%" /ST 02:00 /RL HIGHEST
echo Tarea programada para ejecutarse diariamente a las 2:00 AM
pause