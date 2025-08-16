# Скрипт очистки папок backend-сервисов (удаленная версия)
# Удаляет содержимое папок сервисов на prod сервере

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

# Функция для очистки удаленной папки
function Clear-RemoteServiceFolder {
    param([object]$Service, [object]$ProdServer)
    
    try {
        $networkPath = "\\$($ProdServer.Hostname)\$($ProdServer.RootPath.Replace(':', '$'))"
        $remotePath = Join-Path $networkPath $Service.ProdPath
        
        if (Test-Path $remotePath) {
            $filesToDelete = Get-ChildItem -Path $remotePath -Recurse -ErrorAction SilentlyContinue
            if ($filesToDelete) {
                $filesToDelete | Remove-Item -Force -Recurse -ErrorAction SilentlyContinue
                Write-Log "Папка сервиса $($Service.Name) очищена"
                return $true
            } else {
                Write-Log "Папка сервиса $($Service.Name) уже пуста"
                return $true
            }
        } else {
            Write-Log "Папка сервиса $($Service.Name) не существует"
            return $true
        }
    }
    catch {
        Write-Log "Ошибка при очистке папки сервиса $($Service.Name): $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Основная логика
Write-Log "=== Очистка папок Backend-сервисов (удаленная версия) ==="

# Загружаем конфигурацию
$config = Get-Config -ConfigPath $ConfigPath
Write-Log "Конфигурация загружена из: $ConfigPath"

$successCount = 0
$totalCount = $config.BackendServices.Count

# Очищаем папки всех сервисов
foreach ($service in $config.BackendServices) {
    Write-Log "Очищаю папку сервиса: $($service.Name)"
    
    if (Clear-RemoteServiceFolder -Service $service -ProdServer $config.ProdServer) {
        $successCount++
    }
}

Write-Log "=== Очистка завершена ==="
Write-Log "Очищено папок: $successCount из $totalCount"

if ($successCount -eq $totalCount) {
    Write-Log "Все папки очищены успешно!" "INFO"
} else {
    Write-Log "Очищено $successCount из $totalCount папок" "WARNING"
}
