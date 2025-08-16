@echo off
chcp 65001 >nul
echo === DTJ Services Deployment (Dev Version) ===
echo.
echo Выберите действие:
echo 1. Полное развертывание
echo 2. Только сборка
echo 3. Только развертывание
echo 4. Очистка папок служб
echo 5. Управление службами (состояние/остановка/запуск/перезапуск)
echo.
set /p choice="Введите номер (1-5): "

if "%choice%"=="1" (
    "C:\Program Files\PowerShell\7\pwsh.exe" -ExecutionPolicy Bypass -File deploy-single-server.ps1
) else if "%choice%"=="2" (
    "C:\Program Files\PowerShell\7\pwsh.exe" -ExecutionPolicy Bypass -File deploy-single-server.ps1 -BuildOnly
) else if "%choice%"=="3" (
    "C:\Program Files\PowerShell\7\pwsh.exe" -ExecutionPolicy Bypass -File deploy-single-server.ps1 -DeployOnly
) else if "%choice%"=="4" (
    "C:\Program Files\PowerShell\7\pwsh.exe" -ExecutionPolicy Bypass -File clear-services.ps1
) else if "%choice%"=="5" (
    "C:\Program Files\PowerShell\7\pwsh.exe" -ExecutionPolicy Bypass -File manage-services.ps1
) else (
    echo Неправильный выбор
)

pause
