# Скрипт для исправления app-run.bat файлов во всех сервисах

param(
    [string]$ServicesPath = "C:\dtj\server"
)

Write-Host "=== Исправление app-run.bat файлов ===" -ForegroundColor Green

# Создаем шаблон с правильными настройками Java
$template = @"
@echo off
set JAVA_HOME=C:\tools\jdk17
set PATH=%JAVA_HOME%\bin;%PATH%

rem in JC_JVM java parameters -Dxxx=yyy

set CP=%~dp0lib;%~dp0lib\*

set JVM=
set JVM=%JVM% -cp %CP%
set JVM=%JVM% -Djandcode.app.appdir=%~dp0
set JVM=%JVM% -Djandcode.app.cmdname=%~n0
set JVM=%JVM% -Dfile.encoding=UTF-8
set MAIN=tofi.adm.main.Main

java %JVM% %JC_JVM% %MAIN% %*
"@

# Список сервисов
$services = @(
    "admin",
    "meta", 
    "account",
    "tofidata",
    "cube",
    "nsi",
    "object",
    "plan",
    "personnal",
    "orgstructure",
    "inspection"
)

# Специальные MAIN классы для некоторых сервисов
$mainClasses = @{
    "inspection" = "dtj.inspection.main.Main"
    "admin" = "tofi.adm.main.Main"
    "meta" = "tofi.meta.main.Main"
    "account" = "tofi.account.main.Main"
    "tofidata" = "tofi.tofidata.main.Main"
    "cube" = "tofi.cube.main.Main"
    "nsi" = "tofi.nsi.main.Main"
    "object" = "tofi.object.main.Main"
    "plan" = "tofi.plan.main.Main"
    "personnal" = "tofi.personnal.main.Main"
    "orgstructure" = "tofi.orgstructure.main.Main"
}

foreach ($service in $services) {
    $appRunPath = Join-Path $ServicesPath $service "app-run.bat"
    
    if (Test-Path $appRunPath) {
        # Создаем содержимое для конкретного сервиса
        $mainClass = $mainClasses[$service]
        $content = $template -replace "tofi\.adm\.main\.Main", $mainClass
        
        # Сохраняем файл
        $content | Out-File -FilePath $appRunPath -Encoding ASCII
        Write-Host "Исправлен $service\app-run.bat" -ForegroundColor Green
    } else {
        Write-Host "Файл не найден: $appRunPath" -ForegroundColor Yellow
    }
}

Write-Host "=== Исправление завершено! ===" -ForegroundColor Green
