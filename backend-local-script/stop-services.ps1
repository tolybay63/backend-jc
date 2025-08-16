# Скрипт для остановки всех сервисов DTJ

param(
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

# Функция для остановки процесса по порту
function Stop-ProcessByPort {
    param([int]$Port)
    
    try {
        # Используем netstat для поиска процесса на порту
        $netstatOutput = netstat -ano | Select-String ":$Port\s"
        
        if ($netstatOutput) {
            $processId = ($netstatOutput -split '\s+')[-1]
            
            if ($processId -and $processId -match '^\d+$') {
                $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
                if ($process) {
                    Write-Log "Останавливаю процесс $($process.ProcessName) (PID: $processId) на порту $Port"
                    Stop-Process -Id $processId -Force
                    return $true
                }
            }
        }
        
        Write-Log "Процесс на порту $Port не найден" "WARNING"
        return $false
    }
    catch {
        $errorMsg = $_.Exception.Message
        Write-Log "Ошибка при остановке процесса на порту $Port: $errorMsg" "ERROR"
        return $false
    }
}

# Основной скрипт
Write-Log "Начинаю остановку всех сервисов DTJ..."

# Загружаем конфигурацию
$config = Get-Config -ConfigPath $ConfigPath

# Останавливаем все сервисы
Write-Log "Останавливаю все сервисы..."
foreach ($service in $config.ProdServices) {
    $port = $service.Port
    $serviceName = $service.Name
    
    Write-Log "Останавливаю сервис $serviceName на порту $port"
    if (Stop-ProcessByPort -Port $port) {
        Write-Log "Сервис $serviceName остановлен"
    } else {
        Write-Log "Не удалось остановить сервис $serviceName" "WARNING"
    }
    
    # Небольшая пауза между остановками
    Start-Sleep -Seconds 1
}

Write-Log "Остановка сервисов завершена"

# Проверяем, что все порты свободны
Write-Log "Проверяю освобождение портов..."
foreach ($service in $config.ProdServices) {
    $port = $service.Port
    $netstatOutput = netstat -ano | Select-String ":$port\s"
    
    if ($netstatOutput) {
        Write-Log "Порт $port все еще занят" "WARNING"
    } else {
        Write-Log "Порт $port свободен"
    }
}


