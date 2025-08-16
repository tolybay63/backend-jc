# Скрипт остановки backend-сервисов (удаленная версия)
# Останавливает все backend-сервисы на prod сервере

param(
    [string]$ConfigPath = "backend-remote-config.json"
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

# Функция для создания учетных данных
function Get-Credentials {
    param([object]$Server)
    
    try {
        $securePassword = ConvertTo-SecureString $Server.Credentials.Password -AsPlainText -Force
        $credentials = New-Object System.Management.Automation.PSCredential($Server.Credentials.Username, $securePassword)
        return $credentials
    }
    catch {
        Write-Log "Ошибка создания учетных данных для $($Server.Hostname): $($_.Exception.Message)" "ERROR"
        return $null
    }
}

# Функция для остановки удаленного сервиса
function Stop-RemoteService {
    param([object]$Service, [object]$ProdServer)
    
    try {
        $credentials = Get-Credentials -Server $ProdServer
        if (-not $credentials) {
            return $false
        }
        
        $session = New-PSSession -ComputerName $ProdServer.Hostname -Credential $credentials
        
        $remoteScript = {
            param($Port)
            
            $netstatOutput = netstat -ano | Select-String ":${Port}\s" | Select-String "LISTENING"
            
            if ($netstatOutput) {
                $processId = ($netstatOutput -split '\s+')[-1]
                
                if ($processId -and $processId -match '^\d+$') {
                    $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
                    if ($process) {
                        Write-Host "Останавливаю процесс $($process.ProcessName) (PID: $processId) на порту ${Port}"
                        Stop-Process -Id $processId -Force
                        return $true
                    }
                }
            }
            
            Write-Host "Процесс на порту ${Port} не найден"
            return $false
        }
        
        $result = Invoke-Command -Session $session -ScriptBlock $remoteScript -ArgumentList $Service.Port
        Remove-PSSession $session
        
        return $result
    }
    catch {
        Write-Log "Ошибка при остановке сервиса $($Service.Name): $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Основная логика
Write-Log "=== Остановка Backend-сервисов (удаленная версия) ==="

# Загружаем конфигурацию
$config = Get-Config -ConfigPath $ConfigPath
Write-Log "Конфигурация загружена из: $ConfigPath"

$stoppedCount = 0
$totalCount = $config.BackendServices.Count

# Останавливаем все сервисы
foreach ($service in $config.BackendServices) {
    Write-Log "Останавливаю сервис: $($service.Name) (порт: $($service.Port))"
    
    if (Stop-RemoteService -Service $service -ProdServer $config.ProdServer) {
        $stoppedCount++
    }
}

Write-Log "=== Остановка завершена ==="
Write-Log "Остановлено сервисов: $stoppedCount из $totalCount"

if ($stoppedCount -eq $totalCount) {
    Write-Log "Все сервисы остановлены успешно!" "INFO"
} else {
    Write-Log "Остановлено $stoppedCount из $totalCount сервисов" "WARNING"
}
