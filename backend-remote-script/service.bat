@echo off
setlocal enabledelayedexpansion

echo === Service Management Script ===
echo.

REM Проверяем параметры
if "%1"=="" (
    echo Usage: service.bat [stop^|run] [all^|service_name]
    echo.
    echo Examples:
    echo   service.bat stop all
    echo   service.bat run all  
    echo   service.bat stop cube
    echo   service.bat run nsi
    echo.
    echo Available services: admin meta account tofidata cube nsi object plan personnal orgstructure inspection
    pause
    exit /b 1
)

if "%2"=="" (
    echo Error: Missing second parameter
    echo Usage: service.bat [stop^|run] [all^|service_name]
    pause
    exit /b 1
)

set ACTION=%1
set SERVICE=%2

REM Загружаем конфигурацию
for /f "tokens=2 delims=:" %%a in ('findstr "IP" backend-remote-config.json ^| findstr "192.168.1.20"') do set PROD_SERVER=%%a
set PROD_SERVER=%PROD_SERVER:"=%
set PROD_SERVER=%PROD_SERVER:,=%
set PROD_SERVER=%PROD_SERVER: =%

REM Устанавливаем IP сервера напрямую
set PROD_SERVER=192.168.1.20

echo Action: %ACTION%
echo Service: %SERVICE%
echo Prod Server: %PROD_SERVER%
echo.

REM Устанавливаем сетевое подключение
echo Setting up network connection...
net use \\%PROD_SERVER%\C$ /user:%USERNAME% 2>nul
if errorlevel 1 (
    echo Network connection failed
    pause
    exit /b 1
)

if /i "%ACTION%"=="stop" (
    if /i "%SERVICE%"=="all" (
        echo Stopping all services...
        call :stop_all_services
    ) else (
        echo Stopping service: %SERVICE%
        call :stop_single_service %SERVICE%
    )
) else if /i "%ACTION%"=="run" (
    if /i "%SERVICE%"=="all" (
        echo Starting all services...
        call :start_all_services
    ) else (
        echo Starting service: %SERVICE%
        call :start_single_service %SERVICE%
    )
) else (
    echo Error: Invalid action. Use 'stop' or 'run'
    pause
    exit /b 1
)

echo.
echo === Service management completed ===
pause
exit /b 0

:stop_all_services
echo Creating stop script on server...
(
echo @echo off
echo echo === Stopping all services ===
echo.
echo echo Current processes on ports 9172-9182:
echo netstat -ano ^| findstr ":917"
echo.
echo echo Stopping Java processes...
echo taskkill /f /im java.exe 2^>nul
echo.
echo echo Stopping cmd windows...
echo taskkill /f /fi "WINDOWTITLE eq *Service*" 2^>nul
echo taskkill /f /fi "WINDOWTITLE eq *Admin*" 2^>nul
echo taskkill /f /fi "WINDOWTITLE eq *Meta*" 2^>nul
echo taskkill /f /fi "WINDOWTITLE eq *Account*" 2^>nul
echo taskkill /f /fi "WINDOWTITLE eq *Tofidata*" 2^>nul
echo taskkill /f /fi "WINDOWTITLE eq *Cube*" 2^>nul
echo taskkill /f /fi "WINDOWTITLE eq *NSI*" 2^>nul
echo taskkill /f /fi "WINDOWTITLE eq *Object*" 2^>nul
echo taskkill /f /fi "WINDOWTITLE eq *Plan*" 2^>nul
echo taskkill /f /fi "WINDOWTITLE eq *Personnal*" 2^>nul
echo taskkill /f /fi "WINDOWTITLE eq *Orgstructure*" 2^>nul
echo taskkill /f /fi "WINDOWTITLE eq *Inspection*" 2^>nul
echo.
echo echo === Stop completed ===
echo pause
) > "\\%PROD_SERVER%\C$\stop_all_services.bat"

echo Executing stop script on server...
psexec \\%PROD_SERVER% cmd /c "C:\stop_all_services.bat"
goto :eof

:stop_single_service
set SERVICE_NAME=%1
set SERVICE_PORT=

REM Определяем порт для сервиса
if /i "%SERVICE_NAME%"=="admin" set SERVICE_PORT=9172
if /i "%SERVICE_NAME%"=="meta" set SERVICE_PORT=9173
if /i "%SERVICE_NAME%"=="account" set SERVICE_PORT=9174
if /i "%SERVICE_NAME%"=="tofidata" set SERVICE_PORT=9175
if /i "%SERVICE_NAME%"=="cube" set SERVICE_PORT=9176
if /i "%SERVICE_NAME%"=="nsi" set SERVICE_PORT=9177
if /i "%SERVICE_NAME%"=="object" set SERVICE_PORT=9178
if /i "%SERVICE_NAME%"=="plan" set SERVICE_PORT=9179
if /i "%SERVICE_NAME%"=="personnal" set SERVICE_PORT=9180
if /i "%SERVICE_NAME%"=="orgstructure" set SERVICE_PORT=9181
if /i "%SERVICE_NAME%"=="inspection" set SERVICE_PORT=9182

if "%SERVICE_PORT%"=="" (
    echo Error: Unknown service %SERVICE_NAME%
    goto :eof
)

echo Creating stop script for %SERVICE_NAME% on port %SERVICE_PORT%...
(
echo @echo off
echo echo === Stopping %SERVICE_NAME% service ===
echo.
echo echo Current processes on port %SERVICE_PORT%:
echo netstat -ano ^| findstr ":%SERVICE_PORT%"
echo.
echo echo Stopping %SERVICE_NAME% on port %SERVICE_PORT%:
echo for /f "tokens=5" %%i in ^('netstat -ano ^| findstr ":%SERVICE_PORT%"'^) do taskkill /f /pid %%i 2^>nul
echo.
echo echo === Stop completed ===
echo pause
) > "\\%PROD_SERVER%\C$\stop_%SERVICE_NAME%.bat"

echo Executing stop script on server...
psexec \\%PROD_SERVER% cmd /c "C:\stop_%SERVICE_NAME%.bat"
goto :eof

:start_all_services
echo Creating start script on server...
(
echo @echo off
echo set JAVA_HOME=C:\Users\Tolybay.Kuanov\jc2\java17
echo set PATH=%%JAVA_HOME%%\bin
echo.
echo set CUR_DIR=C:\dtj\server
echo.
echo echo === Starting all services ===
echo.
echo echo Starting Admin Service...
echo start "Admin Service" %%CUR_DIR%%\admin\app-run.bat serve -c / -p 9172 -log
echo.
echo echo Starting Meta Service...
echo start "Meta Service" %%CUR_DIR%%\meta\app-run.bat serve -c / -p 9173 -log
echo.
echo echo Starting Data User ^(Account^) Service...
echo start "Account Service" %%CUR_DIR%%\account\app-run.bat serve -c / -p 9174 -log
echo.
echo echo Starting TOFI Data Service...
echo start "Tofidata Service" %%CUR_DIR%%\tofidata\app-run.bat serve -c / -p 9175 -log
echo.
echo echo Starting Cube Service...
echo start "Cube Service" %%CUR_DIR%%\cube\app-run.bat serve -c / -p 9176 -log
echo.
echo echo Starting NSI Service...
echo start "NSI Service" %%CUR_DIR%%\nsi\app-run.bat serve -c / -p 9177 -log
echo.
echo echo Starting Object Service...
echo start "Object Service" %%CUR_DIR%%\object\app-run.bat serve -c / -p 9178 -log
echo.
echo echo Starting Plan Service...
echo start "Plan Service" %%CUR_DIR%%\plan\app-run.bat serve -c / -p 9179 -log
echo.
echo echo Starting Personnal Service...
echo start "Personnal Service" %%CUR_DIR%%\personnal\app-run.bat serve -c / -p 9180 -log
echo.
echo echo Starting OrgStructure Service...
echo start "OrgStructure Service" %%CUR_DIR%%\orgstructure\app-run.bat serve -c / -p 9181 -log
echo.
echo echo Starting Inspection Service...
echo start "Inspection Service" %%CUR_DIR%%\inspection\app-run.bat serve -c / -p 9182 -log
echo.
echo echo === Start completed ===
echo pause
) > "\\%PROD_SERVER%\C$\start_all_services.bat"

echo Executing start script on server...
psexec \\%PROD_SERVER% cmd /c "C:\start_all_services.bat"
goto :eof

:start_single_service
set SERVICE_NAME=%1
set SERVICE_PATH=

REM Определяем путь для сервиса
if /i "%SERVICE_NAME%"=="admin" set SERVICE_PATH=admin
if /i "%SERVICE_NAME%"=="meta" set SERVICE_PATH=meta
if /i "%SERVICE_NAME%"=="account" set SERVICE_PATH=account
if /i "%SERVICE_NAME%"=="tofidata" set SERVICE_PATH=tofidata
if /i "%SERVICE_NAME%"=="cube" set SERVICE_PATH=cube
if /i "%SERVICE_NAME%"=="nsi" set SERVICE_PATH=nsi
if /i "%SERVICE_NAME%"=="object" set SERVICE_PATH=object
if /i "%SERVICE_NAME%"=="plan" set SERVICE_PATH=plan
if /i "%SERVICE_NAME%"=="personnal" set SERVICE_PATH=personnal
if /i "%SERVICE_NAME%"=="orgstructure" set SERVICE_PATH=orgstructure
if /i "%SERVICE_NAME%"=="inspection" set SERVICE_PATH=inspection

if "%SERVICE_PATH%"=="" (
    echo Error: Unknown service %SERVICE_NAME%
    goto :eof
)

echo Creating start script for %SERVICE_NAME%...
(
echo @echo off
echo set JAVA_HOME=C:\Users\Tolybay.Kuanov\jc2\java17
echo set PATH=%%JAVA_HOME%%\bin
echo.
echo set CUR_DIR=C:\dtj\server
echo.
echo echo === Starting %SERVICE_NAME% service ===
echo.
echo start "%SERVICE_NAME% Service" %%CUR_DIR%%\%SERVICE_PATH%\app-run.bat serve -c / -p %SERVICE_PORT% -log
echo.
echo echo === Start completed ===
echo pause
) > "\\%PROD_SERVER%\C$\start_%SERVICE_NAME%.bat"

echo Executing start script on server...
psexec \\%PROD_SERVER% cmd /c "C:\start_%SERVICE_NAME%.bat"
goto :eof

