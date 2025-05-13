@echo off
SETLOCAL

:: Configuración de rutas
SET PROJECT_ROOT=%~dp0
SET VENV_ACTIVATE="%PROJECT_ROOT%venv\Scripts\activate.bat"
SET VENV_PYTHON="%PROJECT_ROOT%venv\Scripts\python.exe"
SET SCRIPT_PATH="%PROJECT_ROOT%app.py"

:: Verificar que existe activate.bat
if not exist %VENV_ACTIVATE% (
    echo Error: No se encuentra activate.bat en %VENV_ACTIVATE%
    echo Asegurate de que el entorno virtual está correctamente creado.
    pause
    exit /b 1
)

:: Verificar que existe app.py
if not exist %SCRIPT_PATH% (
    echo Error: No se encuentra app.py en %SCRIPT_PATH%
    pause
    exit /b 1
)

:: Activar venv y ejecute app.py
SET TEMP_SCRIPT="%TEMP%\run_starwars_etl.bat"
(
    echo @echo off
    echo CALL %VENV_ACTIVATE%
    echo %VENV_PYTHON% %SCRIPT_PATH%
    echo pause
) > %TEMP_SCRIPT%

:: Tarea para ejecutar el script temporal
SCHTASKS /CREATE /SC DAILY /TN "StarWarsETL" /TR "%TEMP_SCRIPT%" /ST 21:49 /RU "SYSTEM"

echo Tarea programada configurada correctamente:
echo - Se ejecutara: %TEMP_SCRIPT%
echo - Este script activara el venv y luego ejecutará app.py
pause