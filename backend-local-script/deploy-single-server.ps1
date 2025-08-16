# Скрипт развертывания на одном сервере (192.168.0.19)
# Собирает продакшн файлы в каждом сервисе и перезапускает их

param(
    [string]$ConfigPath = "deploy-config.json",
    [switch]$BuildOnly,
    [switch]$DeployOnly,
    [switch]$NoClear,
    [switch]$TestConnection
)

# Функция для логирования
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$timestamp] [$Level] $Message"
}

# Функция для загрузки конфигурации
function Get-Config {
    param([string]$ConfigPath)
    
    if (-not (Test-Path $ConfigPath)) {
        Write-Log "Конфигурационный файл $ConfigPath не найден" "ERROR"
        exit 1
    }
    
    try {
        $config = Get-Content $ConfigPath | ConvertFrom-Json
        return $config
    }
    catch {
        Write-Log "Ошибка при чтении конфигурации: $($_.Exception.Message)" "ERROR"
        exit 1
    }
}

# Функция для остановки процесса по порту
function Stop-ProcessByPort {
    param([int]$Port)
    
    try {
        # Используем netstat для поиска процесса на порту
        $netstatOutput = netstat -ano | Select-String ":${Port}\s" | Select-String "LISTENING"
        
        if ($netstatOutput) {
            $processId = ($netstatOutput -split '\s+')[-1]
            
            if ($processId -and $processId -match '^\d+$') {
                $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
                if ($process) {
                    Write-Log "Останавливаю процесс $($process.ProcessName) (PID: $processId) на порту ${Port}"
                    Stop-Process -Id $processId -Force
                    return $true
                }
            }
        }
        
        Write-Log "Процесс на порту ${Port} не найден" "WARNING"
        return $false
    }
    catch {
        $errorMsg = $_.Exception.Message
        Write-Log "Ошибка при остановке процесса на порту ${Port}: $errorMsg" "ERROR"
        return $false
    }
}

# Функция для сборки сервиса
function Build-Service {
    param([object]$Service, [string]$AppRunPath)
    
    try {
        Write-Log "Собираю сервис $($Service.Name) в папке $($Service.DevPath)"
        
        if (Test-Path $Service.DevPath) {
            # Переходим в папку сервиса и запускаем сборку
            Push-Location $Service.DevPath
            
            # Запускаем jc product для сборки
            $buildResult = & cmd.exe /c "jc product" 2>&1
            $exitCode = $LASTEXITCODE
            
            Pop-Location
            
            if ($exitCode -eq 0) {
                Write-Log "Сборка сервиса $($Service.Name) завершена успешно"
                return $true
            } else {
                Write-Log "Ошибка при сборке сервиса $($Service.Name) (ExitCode: $exitCode)" "ERROR"
                Write-Log "Вывод сборки: $buildResult" "ERROR"
                return $false
            }
        } else {
            Write-Log "Папка сервиса $($Service.Name) не найдена: $($Service.DevPath)" "ERROR"
            return $false
        }
    }
    catch {
        Write-Log "Ошибка при сборке сервиса $($Service.Name)`: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Функция для остановки всех сервисов
function Stop-AllServices {
    param([array]$ProdServices)
    
    try {
        Write-Log "Останавливаю все сервисы..."
        
        foreach ($service in $ProdServices) {
            if (-not (Stop-ProcessByPort -Port $service.Port)) {
                Write-Log "Предупреждение: не удалось остановить сервис $($service.Name) на порту $($service.Port)" "WARNING"
            }
        }
        
        Start-Sleep -Seconds $config.WaitTimeAfterStop
        return $true
    }
    catch {
        Write-Log "Ошибка при остановке сервисов: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Функция для очистки папок сервисов
function Clear-ServiceFolders {
    param([array]$ProdServices)
    
    try {
        Write-Log "Очищаю папки сервисов..."
        
        foreach ($service in $ProdServices) {
            if (Test-Path $service.RemotePath) {
                # Удаляем все файлы и папки, кроме app-run.bat
                Get-ChildItem -Path $service.RemotePath -Recurse | Where-Object { 
                    $_.Name -ne "app-run.bat" 
                } | Remove-Item -Force -Recurse -ErrorAction SilentlyContinue
                Write-Log "Очищена папка для сервиса $($service.Name)"
            } else {
                Write-Log "Папка для сервиса $($service.Name) не найдена: $($service.RemotePath)" "WARNING"
            }
        }
        
        return $true
    }
    catch {
        Write-Log "Ошибка при очистке папок: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Функция для копирования файлов
function Copy-ServiceFiles {
    param([array]$DevServices, [array]$ProdServices)
    
    try {
        Write-Log "Копирую файлы сборки в папки сервисов..."
        
        foreach ($prodService in $ProdServices) {
            # Находим соответствующий dev-сервис
            $devService = $DevServices | Where-Object { $_.Name -eq $prodService.SourceService }
            
            if ($devService) {
                $sourcePath = "$($devService.DevPath)\$($devService.BuildPath)"
                $destPath = $prodService.RemotePath
                
                if (Test-Path $sourcePath) {
                    if (Test-Path $destPath) {
                        # Копируем все содержимое папки product, включая lib
                        Copy-Item -Path "$sourcePath\*" -Destination $destPath -Recurse -Force
                        Write-Log "Скопированы файлы для сервиса $($prodService.Name) (включая lib)"
                    } else {
                        Write-Log "Папка назначения не найдена: $destPath" "ERROR"
                    }
                } else {
                    Write-Log "Папка сборки не найдена: $sourcePath" "ERROR"
                }
            } else {
                Write-Log "Не найден dev-сервис для $($prodService.Name)" "ERROR"
            }
        }
        
        return $true
    }
    catch {
        Write-Log "Ошибка при копировании файлов: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Функция для запуска сервиса
function Start-Service {
    param([object]$Service, [string]$AppRunPath)
    
    try {
        Write-Log "Запускаю сервис $($Service.Name) на порту $($Service.Port)"
        
        if (Test-Path $Service.RemotePath) {
            # Переходим в папку сервиса
            Push-Location $Service.RemotePath
            
            # Запускаем сервис
            $startResult = Start-Process -FilePath "cmd.exe" -ArgumentList "/c", "app-run.bat", "serve", "-c", "/", "-p", $Service.Port -PassThru -WindowStyle Hidden
            
            Pop-Location
            
            if ($startResult) {
                Write-Log "Сервис $($Service.Name) запущен (PID: $($startResult.Id))"
                return $true
            } else {
                Write-Log "Ошибка при запуске сервиса $($Service.Name)" "ERROR"
                return $false
            }
        } else {
            Write-Log "Папка сервиса $($Service.Name) не найдена: $($Service.RemotePath)" "ERROR"
            return $false
        }
    }
    catch {
        Write-Log "Ошибка при запуске сервиса $($Service.Name)`: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Функция для запуска всех сервисов
function Start-AllServices {
    param([array]$ProdServices, [string]$AppRunPath)
    
    try {
        Write-Log "Запускаю все сервисы..."
        
        foreach ($service in $ProdServices) {
            if (-not (Start-Service -Service $service -AppRunPath $AppRunPath)) {
                Write-Log "Ошибка при запуске сервиса $($service.Name)" "ERROR"
            }
            Start-Sleep -Seconds $config.WaitTimeAfterStart
        }
        
        return $true
    }
    catch {
        Write-Log "Ошибка при запуске сервисов: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Основной скрипт
Write-Log "Начинаю процесс развертывания на сервере 192.168.0.19..."

# Загружаем конфигурацию
$config = Get-Config -ConfigPath $ConfigPath

# Этап 1: Сборка всех сервисов
if (-not $DeployOnly) {
    Write-Log "=== Этап 1: Сборка сервисов ==="
    
    $buildSuccess = $true
    foreach ($devService in $config.DevServices) {
        if (-not (Build-Service -Service $devService -AppRunPath $config.AppRunPath)) {
            $buildSuccess = $false
            Write-Log "Ошибка при сборке сервиса $($devService.Name)" "ERROR"
        }
    }
    
    if (-not $buildSuccess) {
        Write-Log "Ошибка при сборке сервисов" "ERROR"
        exit 1
    }
    
    Write-Log "Сборка всех сервисов завершена успешно"
}

# Этап 2: Развертывание сервисов
if (-not $BuildOnly) {
    Write-Log "=== Этап 2: Развертывание сервисов ==="
    
    # Останавливаем сервисы
    if (-not (Stop-AllServices -ProdServices $config.ProdServices)) {
        Write-Log "Ошибка при остановке сервисов" "ERROR"
        exit 1
    }
    
    # Очищаем папки сервисов перед копированием
    if (-not $NoClear) {
        if (-not (Clear-ServiceFolders -ProdServices $config.ProdServices)) {
            Write-Log "Ошибка при очистке папок" "ERROR"
            exit 1
        }
    }
    
    # Копируем файлы
    if (-not (Copy-ServiceFiles -DevServices $config.DevServices -ProdServices $config.ProdServices)) {
        Write-Log "Ошибка при копировании файлов" "ERROR"
        exit 1
    }
    
    # Исправляем app-run.bat файлы для Java 17
    Write-Log "Исправляю app-run.bat файлы для Java 17..."
    foreach ($service in $config.ProdServices) {
        $appRunPath = "$($service.RemotePath)\app-run.bat"
        if (Test-Path $appRunPath) {
            $content = Get-Content $appRunPath -Raw
            if ($content -notmatch "set JAVA_HOME=C:\\tools\\jdk17") {
                $newContent = $content -replace "@echo off", "@echo off`n`nset JAVA_HOME=C:\tools\jdk17`nset PATH=%JAVA_HOME%\bin;%PATH%`n"
                Set-Content $appRunPath $newContent -Encoding ASCII
                Write-Log "Исправлен $($service.Name)\app-run.bat"
            }
        }
    }
    
    # Запускаем сервисы
    if (-not (Start-AllServices -ProdServices $config.ProdServices -AppRunPath $config.AppRunPath)) {
        Write-Log "Ошибка при запуске сервисов" "ERROR"
        exit 1
    }
    
    Write-Log "Развертывание сервисов завершено успешно"
}

Write-Log "=== Развертывание завершено! ==="
Write-Log "Запущенные сервисы:"
foreach ($service in $config.ProdServices) {
    Write-Log "  - $($service.Name) на порту $($service.Port)"
}
