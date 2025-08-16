# Универсальный скрипт для управления сервисами DTJ

param(
    [string]$Action = "status",  # status, stop, start, restart, kill
    [string]$ServiceNames = "all",  # all или конкретные сервисы через запятую
    [string]$ConfigPath = "deploy-config.json"
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
        $errorMsg = $_.Exception.Message
        Write-Log "Ошибка при чтении конфигурации: $errorMsg" "ERROR"
        exit 1
    }
}

# Функция для получения статуса сервисов
function Get-ServicesStatus {
    param([object]$Config)
    
    Write-Log "=== Статус сервисов DTJ ==="
    Write-Log "Порт | Сервис | Статус | PID"
    Write-Log "-----|--------|--------|-----"
    
    foreach ($service in $config.ProdServices) {
        $port = $service.Port
        $serviceName = $service.Name
        $netstatOutput = netstat -ano | Select-String ":${port}\s" | Select-String "LISTENING"
        
        if ($netstatOutput) {
            $processId = ($netstatOutput -split '\s+')[-1]
            Write-Log "${port} | $serviceName | ЗАПУЩЕН | $processId"
        } else {
            Write-Log "${port} | $serviceName | ОСТАНОВЛЕН | -"
        }
    }
}

# Функция для остановки процесса по порту
function Stop-ProcessByPort {
    param([int]$Port)
    
    try {
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

# Функция для запуска сервиса
function Start-Service {
    param([object]$Service)
    
    try {
        $servicePath = $Service.RemotePath
        $port = $Service.Port
        $serviceName = $Service.Name
        
        if (-not (Test-Path $servicePath)) {
            Write-Log "Папка сервиса $serviceName не найдена: $servicePath" "ERROR"
            return $false
        }
        
        $appRunPath = Join-Path $servicePath "app-run.bat"
        if (-not (Test-Path $appRunPath)) {
            Write-Log "Файл app-run.bat не найден в папке сервиса $serviceName" "ERROR"
            return $false
        }
        
        Write-Log "Запускаю сервис $serviceName на порту ${port}"
        
        $process = Start-Process -FilePath "cmd.exe" -ArgumentList "/c", "app-run.bat", "serve", "-c", "/", "-p", $port -WorkingDirectory $servicePath -PassThru -WindowStyle Hidden
        
        if ($process) {
            Write-Log "Сервис $serviceName запущен (PID: $($process.Id))"
            return $true
        } else {
            Write-Log "Не удалось запустить сервис $serviceName" "ERROR"
            return $false
        }
    }
    catch {
        $errorMsg = $_.Exception.Message
        Write-Log "Ошибка при запуске сервиса $($Service.Name): $errorMsg" "ERROR"
        return $false
    }
}

# Функция для перезапуска сервиса
function Restart-Service {
    param([object]$Service)
    
    $serviceName = $Service.Name
    $port = $Service.Port
    
    Write-Log "=== Перезапуск сервиса $serviceName ==="
    
    # Останавливаем сервис
    Write-Log "Останавливаю сервис $serviceName на порту ${port}"
    if (Stop-ProcessByPort -Port $port) {
        Write-Log "Сервис $serviceName остановлен"
    } else {
        Write-Log "Сервис $serviceName не был запущен или уже остановлен" "WARNING"
    }
    
    # Ждем немного
    Start-Sleep -Seconds 2
    
    # Запускаем сервис
    if (Start-Service -Service $Service) {
        Write-Log "Сервис $serviceName успешно перезапущен"
        return $true
    } else {
        Write-Log "Ошибка при запуске сервиса $serviceName" "ERROR"
        return $false
    }
}

# Основной скрипт
Write-Log "Начинаю управление сервисами DTJ..."

# Показываем справку, если действие не указано или неправильное
if (-not $Action -or $Action -notmatch "^(status|stop|start|restart|kill)$") {
    Write-Log "=== СПРАВКА ПО ИСПОЛЬЗОВАНИЮ ===" "INFO"
    Write-Log "Использование: .\manage-services.ps1 -Action <действие> [-ServiceNames <сервисы>]" "INFO"
    Write-Log "" "INFO"
    Write-Log "Действия:" "INFO"
    Write-Log "  status   - показать статус всех сервисов" "INFO"
    Write-Log "  stop     - остановить сервисы" "INFO"
    Write-Log "  start    - запустить сервисы" "INFO"
    Write-Log "  restart  - перезапустить сервисы" "INFO"
    Write-Log "  kill     - принудительно остановить сервисы" "INFO"
    Write-Log "" "INFO"
    Write-Log "Примеры:" "INFO"
    Write-Log "  .\manage-services.ps1 -Action status" "INFO"
    Write-Log "  .\manage-services.ps1 -Action restart -ServiceNames nsi,admin" "INFO"
    Write-Log "  .\manage-services.ps1 -Action stop -ServiceNames all" "INFO"
    Write-Log "" "INFO"
    Write-Log "Доступные сервисы: admin, meta, account, tofidata, cube, nsi, object, plan, personnal, orgstructure, inspection" "INFO"
    exit 1
}

# Загружаем конфигурацию
$config = Get-Config -ConfigPath $ConfigPath

# Определяем, какие сервисы обрабатывать
$servicesToProcess = @()
if ($ServiceNames -eq "all") {
    $servicesToProcess = $config.ProdServices
} else {
    $serviceNamesArray = $ServiceNames -split ","
    foreach ($serviceName in $serviceNamesArray) {
        $service = $config.ProdServices | Where-Object { $_.Name -eq $serviceName.Trim() }
        if ($service) {
            $servicesToProcess += $service
        } else {
            Write-Log "Сервис '$serviceName' не найден в конфигурации" "WARNING"
        }
    }
}

# Выполняем действие
switch ($Action.ToLower()) {
    "status" {
        Get-ServicesStatus -Config $config
    }
    "stop" {
        Write-Log "Останавливаю сервисы: $($servicesToProcess.Name -join ', ')"
        foreach ($service in $servicesToProcess) {
            $port = $service.Port
            $serviceName = $service.Name
            Write-Log "Останавливаю сервис $serviceName на порту ${port}"
            Stop-ProcessByPort -Port $port
            Start-Sleep -Seconds 1
        }
        Write-Log "Остановка сервисов завершена"
    }
    "start" {
        Write-Log "Запускаю сервисы: $($servicesToProcess.Name -join ', ')"
        foreach ($service in $servicesToProcess) {
            Start-Service -Service $service
            Start-Sleep -Seconds 3
        }
        Write-Log "Запуск сервисов завершен"
    }
    "restart" {
        Write-Log "Перезапускаю сервисы: $($servicesToProcess.Name -join ', ')"
        $successCount = 0
        foreach ($service in $servicesToProcess) {
            if (Restart-Service -Service $service) {
                $successCount++
            }
            Start-Sleep -Seconds 3
        }
        Write-Log "Перезапуск завершен. Успешно: $successCount из $($servicesToProcess.Count)"
    }
    "kill" {
        Write-Log "Принудительно останавливаю сервисы: $($servicesToProcess.Name -join ', ')"
        foreach ($service in $servicesToProcess) {
            $port = $service.Port
            $serviceName = $service.Name
            Write-Log "Принудительно останавливаю сервис $serviceName на порту ${port}"
            
            # Находим все процессы на порту и убиваем их
            $netstatOutput = netstat -ano | Select-String ":${port}\s"
            foreach ($line in $netstatOutput) {
                $processId = ($line -split '\s+')[-1]
                if ($processId -and $processId -match '^\d+$') {
                    try {
                        taskkill /PID $processId /F
                        Write-Log "Процесс $processId принудительно остановлен"
                    }
                    catch {
                        Write-Log "Не удалось остановить процесс $processId" "WARNING"
                    }
                }
            }
            Start-Sleep -Seconds 1
        }
        Write-Log "Принудительная остановка завершена"
    }
    default {
        Write-Log "Неизвестное действие: $Action" "ERROR"
        Write-Log "Доступные действия: status, stop, start, restart, kill"
        exit 1
    }
}

Write-Log "Операция завершена"

