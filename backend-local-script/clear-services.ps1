# Скрипт для очистки папок сервисов на сервере 192.168.0.19

param(
    [string]$ConfigPath = "deploy-config.json",
    [switch]$Confirm
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

# Функция для остановки процессов на портах
function Stop-ProcessByPort {
    param([int]$Port)
    
    try {
        # Используем netstat для поиска процессов на порту
        $netstatOutput = netstat -ano | Select-String ":$Port\s"
        
        if ($netstatOutput) {
            foreach ($line in $netstatOutput) {
                $parts = $line -split '\s+'
                $pid = $parts[-1]
                
                if ($pid -and $pid -ne "0") {
                    Write-Log "Останавливаю процесс PID $pid на порту $Port"
                    taskkill /PID $pid /F /T 2>$null
                }
            }
        }
    }
    catch {
        $errorMsg = $_.Exception.Message
        Write-Log "Ошибка при остановке процесса на порту ${Port}: $errorMsg" "ERROR"
    }
}

# Функция для остановки всех сервисов
function Stop-AllServices {
    param([array]$Services)
    
    Write-Log "Останавливаю все сервисы перед очисткой..."
    
    # Сначала останавливаем по портам
    foreach ($service in $Services) {
        $port = $service.Port
        if ($port) {
            Write-Log "Останавливаю сервис $($service.Name) на порту $port"
            Stop-ProcessByPort -Port $port
        }
    }
    
    # Даем время процессам завершиться
    Write-Log "Ждем завершения процессов..."
    Start-Sleep -Seconds 3
    
    # Принудительно останавливаем все Java процессы
    Write-Log "Принудительно останавливаю все Java процессы..."
    $javaProcesses = Get-Process -Name "java" -ErrorAction SilentlyContinue
    if ($javaProcesses) {
        Write-Log "Найдено $($javaProcesses.Count) Java процессов, останавливаю..."
        $javaProcesses | Stop-Process -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
    }
    
    # Также останавливаем процессы app-run.bat
    Write-Log "Останавливаю процессы app-run.bat..."
    $batProcesses = Get-Process -Name "cmd" -ErrorAction SilentlyContinue | Where-Object { $_.ProcessName -eq "cmd" }
    if ($batProcesses) {
        Write-Log "Найдено $($batProcesses.Count) cmd процессов, останавливаю..."
        $batProcesses | Stop-Process -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
    }
}

# Функция для очистки папок сервисов
function Clear-ServiceFolders {
    param([array]$Services)
    
    try {
        Write-Log "Очищаю папки сервисов на сервере 192.168.0.19"
        
        foreach ($service in $Services) {
            $servicePath = $service.RemotePath
            
            if (Test-Path $servicePath) {
                Write-Log "Обрабатываю папку: $servicePath"
                
                # Получаем список всех файлов для удаления
                $filesToDelete = Get-ChildItem -Path $servicePath -Recurse -ErrorAction SilentlyContinue
                
                if ($filesToDelete) {
                    Write-Log "Найдено $($filesToDelete.Count) файлов/папок в $($service.Name)"
                    
                    # Показываем что будем удалять
                    $filesToDelete | ForEach-Object { Write-Log "  Удаляю: $($_.FullName)" }
                    
                    # Удаляем ВСЕ файлы и папки (включая app-run.bat и lib)
                    # Используем более агрессивный подход
                    foreach ($file in $filesToDelete) {
                        try {
                            if ($file.PSIsContainer) {
                                # Для папок используем Remove-Item с Force
                                Remove-Item -Path $file.FullName -Force -Recurse -ErrorAction Stop
                            } else {
                                # Для файлов используем Remove-Item с Force
                                Remove-Item -Path $file.FullName -Force -ErrorAction Stop
                            }
                        }
                        catch {
                            Write-Log "Ошибка при удалении $($file.FullName): $($_.Exception.Message)" "WARNING"
                        }
                    }
                    
                    # Проверяем что папка действительно очищена
                    $remainingFiles = Get-ChildItem -Path $servicePath -Recurse -ErrorAction SilentlyContinue
                    if ($remainingFiles) {
                        Write-Log "ВНИМАНИЕ: В папке $($service.Name) остались файлы: $($remainingFiles.Count)" "WARNING"
                    } else {
                        Write-Log "Папка $($service.Name) полностью очищена"
                    }
                } else {
                    Write-Log "Папка $($service.Name) уже пуста"
                }
            } else {
                Write-Log "Папка для сервиса $($service.Name) не найдена: $servicePath" "WARNING"
            }
        }
        
        return $true
    }
    catch {
        Write-Log "Ошибка при очистке папок: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Основной скрипт
Write-Log "Начинаю очистку папок сервисов..."

# Загружаем конфигурацию
$config = Get-Config -ConfigPath $ConfigPath

# Запрашиваем подтверждение
if (-not $Confirm) {
    Write-Host "ВНИМАНИЕ! Вы собираетесь удалить ВСЕ файлы из папок сервисов на сервере 192.168.0.19" -ForegroundColor Red
    Write-Host "Включая app-run.bat, папку lib и все остальные файлы!" -ForegroundColor Red
    Write-Host "Сервисы: $($config.ProdServices.Name -join ', ')" -ForegroundColor Yellow
    $confirmation = Read-Host "Продолжить? (y/N)"
    
    if ($confirmation -ne "y" -and $confirmation -ne "Y") {
        Write-Log "Операция отменена пользователем"
        exit 0
    }
}

# Сначала останавливаем все сервисы
Stop-AllServices -Services $config.ProdServices

# Очищаем папки
if (Clear-ServiceFolders -Services $config.ProdServices) {
    Write-Log "Очистка папок сервисов завершена успешно"
} else {
    Write-Log "Ошибка при очистке папок" "ERROR"
    exit 1
}
