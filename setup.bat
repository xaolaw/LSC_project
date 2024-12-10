@ECHO OFF

REM creating virtual environment
CALL python -m venv .venv
CALL .\\.venv\\Scripts\\activate
IF %ERRORLEVEL% NEQ 0 (echo ERROR:%ERRORLEVEL% && exit)

REM installing packages
CALL pip install -r .\requirements.txt
IF %ERRORLEVEL% NEQ 0 (echo ERROR:%ERRORLEVEL% && exit)
