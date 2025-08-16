# Скрипт исправления app-run.bat (удаленная версия)
# Исправляет права доступа к app-run.bat на prod сервере

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

# Функция для исправления app-run.bat на удаленном сервере
function Fix-RemoteAppRun {
    param([object]$Service, [object]$ProdServer)
    
    try {
        $credentials = Get-Credentials -Server $ProdServer
        if (-not $credentials) {
            return $false
        }
        
        $session = New-PSSession -ComputerName $ProdServer.Hostname -Credential $credentials
        
        $remoteScript = {
            param($ServicePath)
            
            $appRunPath = Join-Path $ServicePath "app-run.bat"
            
            if (Test-Path $appRunPath) {
                # Устанавливаем права на выполнение
                $acl = Get-Acl $appRunPath
                $rule = New-Object System.Security.AccessControl.FileSystemAccessRule("Everyone", "FullControl", "Allow")
                $acl.SetAccessRule($rule)
                Set-Acl $appRunPath $acl
                
                Write-Host "Права доступа к app-run.bat исправлены"
                return $true
            } else {
                Write-Host "Файл app-run.bat не найден: $appRunPath"
                return $false
            }
        }
        
        $prodPath = Join-Path $ProdServer.RootPath $Service.ProdPath
        $result = Invoke-Command -Session $session -ScriptBlock $remoteScript -ArgumentList $prodPath
        Remove-PSSession $session
        
        return $result
    }
    catch {
        Write-Log "Ошибка при исправлении app-run.bat для сервиса $($Service.Name): $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Основная логика
Write-Log "=== Исправление app-run.bat (удаленная версия) ==="

# Загружаем конфигурацию
$config = Get-Config -ConfigPath $ConfigPath
Write-Log "Конфигурация загружена из: $ConfigPath"

$fixedCount = 0
$totalCount = $config.BackendServices.Count

# Исправляем app-run.bat для всех сервисов
foreach ($service in $config.BackendServices) {
    Write-Log "Исправляю app-run.bat для сервиса: $($service.Name)"
    
    if (Fix-RemoteAppRun -Service $service -ProdServer $config.ProdServer) {
        $fixedCount++
    }
}

Write-Log "=== Исправление завершено ==="
Write-Log "Исправлено файлов: $fixedCount из $totalCount"

if ($fixedCount -eq $totalCount) {
    Write-Log "Все файлы app-run.bat исправлены успешно!" "INFO"
} else {
    Write-Log "Исправлено $fixedCount из $totalCount файлов" "WARNING"
}
