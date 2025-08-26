@echo off
echo === Selective Build Services ===
echo.
echo Parameter 1: [%1]
echo.
if "%1"=="" (
    echo Usage: build-select.bat [service]
    echo Available: admin meta account tofidata cube nsi object plan personnal orgstructure inspection all
    pause
    exit /b 1
)

set DEV_PATH=C:\jc-2\backend-jc

if /i "%1"=="nsi" (
    echo Building nsi...
    cd /d "%DEV_PATH%\dtj\dtj-nsi"
    call jc.bat product
    echo nsi built!
    goto :end
)

if /i "%1"=="cube" (
    echo Building cube...
    cd /d "%DEV_PATH%\tofi-cube"
    call jc.bat product
    echo cube built!
    goto :end
)

if /i "%1"=="admin" (
    echo Building admin...
    cd /d "%DEV_PATH%\tofi-adm"
    call jc.bat product
    echo admin built!
    goto :end
)

if /i "%1"=="meta" (
    echo Building meta...
    cd /d "%DEV_PATH%\tofi-mdl"
    call jc.bat product
    echo meta built!
    goto :end
)

if /i "%1"=="account" (
    echo Building account...
    cd /d "%DEV_PATH%\tofi-userdata"
    call jc.bat product
    echo account built!
    goto :end
)

if /i "%1"=="tofidata" (
    echo Building tofidata...
    cd /d "%DEV_PATH%\tofi-data"
    call jc.bat product
    echo tofidata built!
    goto :end
)

if /i "%1"=="object" (
    echo Building object...
    cd /d "%DEV_PATH%\dtj\dtj-object"
    call jc.bat product
    echo object built!
    goto :end
)

if /i "%1"=="plan" (
    echo Building plan...
    cd /d "%DEV_PATH%\dtj\dtj-plan"
    call jc.bat product
    echo plan built!
    goto :end
)

if /i "%1"=="personnal" (
    echo Building personnal...
    cd /d "%DEV_PATH%\dtj\dtj-personnal"
    call jc.bat product
    echo personnal built!
    goto :end
)

if /i "%1"=="orgstructure" (
    echo Building orgstructure...
    cd /d "%DEV_PATH%\dtj\dtj-orgstructure"
    call jc.bat product
    echo orgstructure built!
    goto :end
)

if /i "%1"=="inspection" (
    echo Building inspection...
    cd /d "%DEV_PATH%\dtj\dtj-inspection"
    call jc.bat product
    echo inspection built!
    goto :end
)

if /i "%1"=="all" (
    echo Building all services...
    call build-all.bat
    goto :end
)

echo Unknown service: %1

:end
echo.
echo === Build completed ===
pause

