# Скрипт управления backend-сервисами (удаленная версия)
# Показывает состояние, останавливает, запускает и перезапускает сервисы на prod сервере

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

# Функция для проверки состояния удаленного сервиса
function Get-RemoteServiceStatus {
    param([object]$Service, [object]$ProdServer)
    
    try {
        $credentials = Get-Credentials -Server $ProdServer
        if (-not $credentials) {
            return $null
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
                        return @{
                            Status = "Running"
                            ProcessName = $process.ProcessName
                            ProcessId = $processId
                            Port = $Port
                        }
                    }
                }
            }
            
            return @{
                Status = "Stopped"
                ProcessName = $null
                ProcessId = $null
                Port = $Port
            }
        }
        
        $result = Invoke-Command -Session $session -ScriptBlock $remoteScript -ArgumentList $Service.Port
        Remove-PSSession $session
        
        return $result
    }
    catch {
        Write-Log "Ошибка при проверке состояния сервиса $($Service.Name): $($_.Exception.Message)" "ERROR"
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

# Функция для запуска удаленного сервиса
function Start-RemoteService {
    param([object]$Service, [object]$ProdServer)
    
    try {
        $credentials = Get-Credentials -Server $ProdServer
        if (-not $credentials) {
            return $false
        }
        
        $session = New-PSSession -ComputerName $ProdServer.Hostname -Credential $credentials
        
        $remoteScript = {
            param($ServicePath)
            
            if (Test-Path $ServicePath) {
                Push-Location $ServicePath
                
                $startResult = & cmd.exe /c "app-run.bat" 2>&1
                $exitCode = $LASTEXITCODE
                
                Pop-Location
                
                if ($exitCode -eq 0) {
                    Write-Host "Сервис запущен успешно"
                    return $true
                } else {
                    Write-Host "Ошибка при запуске сервиса (ExitCode: $exitCode): $startResult"
                    return $false
                }
            } else {
                Write-Host "Папка сервиса не найдена: $ServicePath"
                return $false
            }
        }
        
        $prodPath = Join-Path $ProdServer.RootPath $Service.ProdPath
        $result = Invoke-Command -Session $session -ScriptBlock $remoteScript -ArgumentList $prodPath
        Remove-PSSession $session
        
        return $result
    }
    catch {
        Write-Log "Ошибка при запуске сервиса $($Service.Name): $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Основная логика
Write-Log "=== Управление Backend-сервисами (удаленная версия) ==="

# Загружаем конфигурацию
$config = Get-Config -ConfigPath $ConfigPath
Write-Log "Конфигурация загружена из: $ConfigPath"

# Показываем меню
Write-Host "`nВыберите действие:"
Write-Host "1. Показать состояние всех сервисов"
Write-Host "2. Остановить все сервисы"
Write-Host "3. Запустить все сервисы"
Write-Host "4. Перезапустить все сервисы"
Write-Host "5. Управление отдельным сервисом"
Write-Host "6. Выход"
Write-Host ""

$choice = Read-Host "Введите номер (1-6)"

switch ($choice) {
    "1" {
        Write-Log "=== Состояние сервисов ==="
        foreach ($service in $config.BackendServices) {
            $status = Get-RemoteServiceStatus -Service $service -ProdServer $config.ProdServer
            if ($status) {
                if ($status.Status -eq "Running") {
                    Write-Log "✅ $($service.Name) - Запущен (PID: $($status.ProcessId), Порт: $($status.Port))"
                } else {
                    Write-Log "❌ $($service.Name) - Остановлен (Порт: $($status.Port))"
                }
            } else {
                Write-Log "❓ $($service.Name) - Ошибка проверки состояния"
            }
        }
    }
    
    "2" {
        Write-Log "=== Остановка всех сервисов ==="
        $stoppedCount = 0
        foreach ($service in $config.BackendServices) {
            if (Stop-RemoteService -Service $service -ProdServer $config.ProdServer) {
                $stoppedCount++
            }
        }
        Write-Log "Остановлено сервисов: $stoppedCount из $($config.BackendServices.Count)"
    }
    
    "3" {
        Write-Log "=== Запуск всех сервисов ==="
        $startedCount = 0
        foreach ($service in $config.BackendServices) {
            if (Start-RemoteService -Service $service -ProdServer $config.ProdServer) {
                $startedCount++
            }
        }
        Write-Log "Запущено сервисов: $startedCount из $($config.BackendServices.Count)"
    }
    
    "4" {
        Write-Log "=== Перезапуск всех сервисов ==="
        $restartedCount = 0
        foreach ($service in $config.BackendServices) {
            Stop-RemoteService -Service $service -ProdServer $config.ProdServer | Out-Null
            Start-Sleep -Seconds 2
            if (Start-RemoteService -Service $service -ProdServer $config.ProdServer) {
                $restartedCount++
            }
        }
        Write-Log "Перезапущено сервисов: $restartedCount из $($config.BackendServices.Count)"
    }
    
    "5" {
        Write-Log "=== Управление отдельным сервисом ==="
        Write-Host "`nДоступные сервисы:"
        for ($i = 0; $i -lt $config.BackendServices.Count; $i++) {
            Write-Host "$($i + 1). $($config.BackendServices[$i].Name) (порт: $($config.BackendServices[$i].Port))"
        }
        
        $serviceChoice = Read-Host "`nВведите номер сервиса (1-$($config.BackendServices.Count))"
        $serviceIndex = [int]$serviceChoice - 1
        
        if ($serviceIndex -ge 0 -and $serviceIndex -lt $config.BackendServices.Count) {
            $selectedService = $config.BackendServices[$serviceIndex]
            
            Write-Host "`nДействия для сервиса $($selectedService.Name):"
            Write-Host "1. Показать состояние"
            Write-Host "2. Остановить"
            Write-Host "3. Запустить"
            Write-Host "4. Перезапустить"
            
            $actionChoice = Read-Host "`nВведите номер действия (1-4)"
            
            switch ($actionChoice) {
                "1" {
                    $status = Get-RemoteServiceStatus -Service $selectedService -ProdServer $config.ProdServer
                    if ($status) {
                        if ($status.Status -eq "Running") {
                            Write-Log "✅ $($selectedService.Name) - Запущен (PID: $($status.ProcessId), Порт: $($status.Port))"
                        } else {
                            Write-Log "❌ $($selectedService.Name) - Остановлен (Порт: $($status.Port))"
                        }
                    }
                }
                "2" {
                    Stop-RemoteService -Service $selectedService -ProdServer $config.ProdServer
                }
                "3" {
                    Start-RemoteService -Service $selectedService -ProdServer $config.ProdServer
                }
                "4" {
                    Stop-RemoteService -Service $selectedService -ProdServer $config.ProdServer | Out-Null
                    Start-Sleep -Seconds 2
                    Start-RemoteService -Service $selectedService -ProdServer $config.ProdServer
                }
            }
        }
    }
    
    "6" {
        Write-Log "Выход из управления сервисами"
        exit 0
    }
    
    default {
        Write-Log "Неверный выбор" "ERROR"
    }
}
