@echo off
setlocal enabledelayedexpansion

echo === Deploy Services Script ===
echo.

REM Проверяем параметры
if "%1"=="" (
    echo Usage: deploy.bat [all^|service_name]
    echo.
    echo Examples:
    echo   deploy.bat all
    echo   deploy.bat cube
    echo   deploy.bat nsi
    echo.
    echo Available services: admin meta account tofidata cube nsi object plan personnal orgstructure inspection
    pause
    exit /b 1
)

set SERVICE=%1

REM Загружаем конфигурацию
for /f "tokens=2 delims=:" %%a in ('findstr "IP" backend-remote-config.json ^| findstr "192.168.1.20"') do set PROD_SERVER=%%a
set PROD_SERVER=%PROD_SERVER:"=%
set PROD_SERVER=%PROD_SERVER:,=%
set PROD_SERVER=%PROD_SERVER: =%

for /f "tokens=2 delims=:" %%a in ('findstr "RootPath" backend-remote-config.json ^| findstr "C:\\\\jc-2"') do set DEV_PATH=%%a
set DEV_PATH=%DEV_PATH:"=%
set DEV_PATH=%DEV_PATH:,=%
set DEV_PATH=%DEV_PATH: =%

REM Устанавливаем значения напрямую
set PROD_SERVER=192.168.1.20
set DEV_PATH=C:\jc-2\backend-jc

echo Service: %SERVICE%
echo Dev Path: %DEV_PATH%
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

if /i "%SERVICE%"=="all" (
    echo Deploying all services...
    call :deploy_all_services
) else (
    echo Deploying service: %SERVICE%
    call :deploy_single_service %SERVICE%
)

echo.
echo === Deploy completed ===
pause
exit /b 0

:deploy_all_services
echo Deploying admin...
call :deploy_single_service admin

echo Deploying meta...
call :deploy_single_service meta

echo Deploying account...
call :deploy_single_service account

echo Deploying tofidata...
call :deploy_single_service tofidata

echo Deploying cube...
call :deploy_single_service cube

echo Deploying nsi...
call :deploy_single_service nsi

echo Deploying object...
call :deploy_single_service object

echo Deploying plan...
call :deploy_single_service plan

echo Deploying personnal...
call :deploy_single_service personnal

echo Deploying orgstructure...
call :deploy_single_service orgstructure

echo Deploying inspection...
call :deploy_single_service inspection

goto :eof

:deploy_single_service
set SERVICE_NAME=%1
set SOURCE_PATH=
set TARGET_PATH=

REM Определяем пути для сервиса
if /i "%SERVICE_NAME%"=="admin" (
    set SOURCE_PATH=%DEV_PATH%\tofi-adm\_jc\product
    set TARGET_PATH=C$\dtj\server\admin
)
if /i "%SERVICE_NAME%"=="meta" (
    set SOURCE_PATH=%DEV_PATH%\tofi-mdl\_jc\product
    set TARGET_PATH=C$\dtj\server\meta
)
if /i "%SERVICE_NAME%"=="account" (
    set SOURCE_PATH=%DEV_PATH%\tofi-userdata\_jc\product
    set TARGET_PATH=C$\dtj\server\account
)
if /i "%SERVICE_NAME%"=="tofidata" (
    set SOURCE_PATH=%DEV_PATH%\tofi-data\_jc\product
    set TARGET_PATH=C$\dtj\server\tofidata
)
if /i "%SERVICE_NAME%"=="cube" (
    set SOURCE_PATH=%DEV_PATH%\tofi-cube\_jc\product
    set TARGET_PATH=C$\dtj\server\cube
)
if /i "%SERVICE_NAME%"=="nsi" (
    set SOURCE_PATH=%DEV_PATH%\dtj\dtj-nsi\_jc\product
    set TARGET_PATH=C$\dtj\server\nsi
)
if /i "%SERVICE_NAME%"=="object" (
    set SOURCE_PATH=%DEV_PATH%\dtj\dtj-object\_jc\product
    set TARGET_PATH=C$\dtj\server\object
)
if /i "%SERVICE_NAME%"=="plan" (
    set SOURCE_PATH=%DEV_PATH%\dtj\dtj-plan\_jc\product
    set TARGET_PATH=C$\dtj\server\plan
)
if /i "%SERVICE_NAME%"=="personnal" (
    set SOURCE_PATH=%DEV_PATH%\dtj\dtj-personnal\_jc\product
    set TARGET_PATH=C$\dtj\server\personnal
)
if /i "%SERVICE_NAME%"=="orgstructure" (
    set SOURCE_PATH=%DEV_PATH%\dtj\dtj-orgstructure\_jc\product
    set TARGET_PATH=C$\dtj\server\orgstructure
)
if /i "%SERVICE_NAME%"=="inspection" (
    set SOURCE_PATH=%DEV_PATH%\dtj\dtj-inspection\_jc\product
    set TARGET_PATH=C$\dtj\server\inspection
)

if "%SOURCE_PATH%"=="" (
    echo Error: Unknown service %SERVICE_NAME%
    goto :eof
)

echo Checking source path: %SOURCE_PATH%
if not exist "%SOURCE_PATH%" (
    echo Error: Source path does not exist: %SOURCE_PATH%
    echo Please build the service first using: build-select.bat %SERVICE_NAME%
    goto :eof
)

echo Copying %SERVICE_NAME% from %SOURCE_PATH% to \\%PROD_SERVER%\%TARGET_PATH%...

REM Создаем папку назначения если не существует
if not exist "\\%PROD_SERVER%\%TARGET_PATH%" (
    echo Creating target directory...
    mkdir "\\%PROD_SERVER%\%TARGET_PATH%"
)

REM Копируем файлы
echo Copying files...
xcopy "%SOURCE_PATH%\*" "\\%PROD_SERVER%\%TARGET_PATH%\" /E /Y /Q

if errorlevel 1 (
    echo Error: Failed to copy %SERVICE_NAME%
) else (
    echo Successfully deployed %SERVICE_NAME%
)

goto :eof

