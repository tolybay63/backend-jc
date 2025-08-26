@echo off
echo === Build all backend services ===

if not "%1"=="" (
    echo Parameters detected, calling build-selected.bat...
    call "%~dp0build-selected.bat" %*
    exit /b %errorlevel%
)

echo Loading configuration...
set DEV_PATH=C:\jc-2\backend-jc

echo Dev server path: %DEV_PATH%

echo.
echo Building services...

echo 1. Building admin (tofi-adm)...
cd /d "%DEV_PATH%\tofi-adm"
call jc.bat product
if %errorlevel% neq 0 (
    echo [ERROR] Failed to build admin
    pause
    exit /b 1
)

echo 2. Building meta (tofi-mdl)...
cd /d "%DEV_PATH%\tofi-mdl"
call jc.bat product
if %errorlevel% neq 0 (
    echo [ERROR] Failed to build meta
    pause
    exit /b 1
)

echo 3. Building account (tofi-userdata)...
cd /d "%DEV_PATH%\tofi-userdata"
call jc.bat product
if %errorlevel% neq 0 (
    echo [ERROR] Failed to build account
    pause
    exit /b 1
)

echo 4. Building tofidata (tofi-data)...
cd /d "%DEV_PATH%\tofi-data"
call jc.bat product
if %errorlevel% neq 0 (
    echo [ERROR] Failed to build tofidata
    pause
    exit /b 1
)

echo 5. Building cube (tofi-cube)...
cd /d "%DEV_PATH%\tofi-cube"
call jc.bat product
if %errorlevel% neq 0 (
    echo [ERROR] Failed to build cube
    pause
    exit /b 1
)

echo 6. Building nsi (dtj\dtj-nsi)...
cd /d "%DEV_PATH%\dtj\dtj-nsi"
call jc.bat product
if %errorlevel% neq 0 (
    echo [ERROR] Failed to build nsi
    pause
    exit /b 1
)

echo 7. Building object (dtj\dtj-object)...
cd /d "%DEV_PATH%\dtj\dtj-object"
call jc.bat product
if %errorlevel% neq 0 (
    echo [ERROR] Failed to build object
    pause
    exit /b 1
)

echo 8. Building plan (dtj\dtj-plan)...
cd /d "%DEV_PATH%\dtj\dtj-plan"
call jc.bat product
if %errorlevel% neq 0 (
    echo [ERROR] Failed to build plan
    pause
    exit /b 1
)

echo 9. Building personnal (dtj\dtj-personnal)...
cd /d "%DEV_PATH%\dtj\dtj-personnal"
call jc.bat product
if %errorlevel% neq 0 (
    echo [ERROR] Failed to build personnal
    pause
    exit /b 1
)

echo 10. Building orgstructure (dtj\dtj-orgstructure)...
cd /d "%DEV_PATH%\dtj\dtj-orgstructure"
call jc.bat product
if %errorlevel% neq 0 (
    echo [ERROR] Failed to build orgstructure
    pause
    exit /b 1
)

echo 11. Building inspection (dtj\dtj-inspection)...
cd /d "%DEV_PATH%\dtj\dtj-inspection"
call jc.bat product
if %errorlevel% neq 0 (
    echo [ERROR] Failed to build inspection
    pause
    exit /b 1
)

echo.
echo === Build completed successfully! ===
echo All services built in _jc\product folders
echo.
pause
