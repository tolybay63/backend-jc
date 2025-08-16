# Скрипт удаленного развертывания backend-сервисов
# Собирает продакшн файлы на dev сервере и развертывает на prod сервере

param(
    [string]$ConfigPath = "backend-remote-config.json",
    [switch]$BuildOnly,
    [switch]$DeployOnly,
    [switch]$ClearOnly,
    [switch]$ManageServices,
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

# Функция для проверки сетевого подключения
function Test-NetworkConnection {
    param([string]$Hostname, [string]$IP)
    
    Write-Log "Проверяю подключение к $Hostname ($IP)"
    
    $pingResult = Test-Connection -ComputerName $IP -Count 1 -Quiet
    if (-not $pingResult) {
        Write-Log "Не удается подключиться к $Hostname ($IP)" "ERROR"
        return $false
    }
    
    Write-Log "Подключение к $Hostname успешно"
    return $true
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

# Функция для остановки удаленного процесса по порту
function Stop-RemoteProcessByPort {
    param([int]$Port, [object]$Server)
    
    try {
        $credentials = Get-Credentials -Server $Server
        if (-not $credentials) {
            return $false
        }
        
        $session = New-PSSession -ComputerName $Server.Hostname -Credential $credentials
        
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
        
        $result = Invoke-Command -Session $session -ScriptBlock $remoteScript -ArgumentList $Port
        Remove-PSSession $session
        
        return $result
    }
    catch {
        Write-Log "Ошибка при остановке удаленного процесса на порту ${Port}: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Функция для сборки сервиса на dev сервере
function Build-RemoteService {
    param([object]$Service, [object]$DevServer)
    
    try {
        $credentials = Get-Credentials -Server $DevServer
        if (-not $credentials) {
            return $false
        }
        
        $session = New-PSSession -ComputerName $DevServer.Hostname -Credential $credentials
        
        $remoteScript = {
            param($ServicePath)
            
            if (Test-Path $ServicePath) {
                Push-Location $ServicePath
                
                $buildResult = & cmd.exe /c "jc product" 2>&1
                $exitCode = $LASTEXITCODE
                
                Pop-Location
                
                if ($exitCode -eq 0) {
                    Write-Host "Сборка завершена успешно"
                    return $true
                } else {
                    Write-Host "Ошибка при сборке (ExitCode: $exitCode): $buildResult"
                    return $false
                }
            } else {
                Write-Host "Папка сервиса не найдена: $ServicePath"
                return $false
            }
        }
        
        $devPath = Join-Path $DevServer.RootPath $Service.DevPath
        $result = Invoke-Command -Session $session -ScriptBlock $remoteScript -ArgumentList $devPath
        Remove-PSSession $session
        
        return $result
    }
    catch {
        Write-Log "Ошибка при сборке удаленного сервиса $($Service.Name): $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Функция для копирования сервиса по SMB
function Copy-RemoteServiceSMB {
    param([object]$Service, [object]$DevServer, [object]$ProdServer)
    
    try {
        $devPath = Join-Path $DevServer.RootPath $Service.DevPath
        $buildPath = Join-Path $devPath "build"
        $prodPath = Join-Path $ProdServer.RootPath $Service.ProdPath
        
        $networkPath = "\\$($ProdServer.Hostname)\$($ProdServer.RootPath.Replace(':', '$'))"
        $remotePath = Join-Path $networkPath $Service.ProdPath
        
        if (-not (Test-Path $buildPath)) {
            Write-Log "Папка build не найдена: $buildPath" "ERROR"
            return $false
        }
        
        if (-not (Test-Path $remotePath)) {
            New-Item -ItemType Directory -Path $remotePath -Force | Out-Null
            Write-Log "Создана папка: $remotePath"
        }
        
        Copy-Item -Path "$buildPath\*" -Destination $remotePath -Recurse -Force
        Write-Log "Сервис $($Service.Name) скопирован успешно"
        return $true
    }
    catch {
        Write-Log "Ошибка при копировании сервиса $($Service.Name): $($_.Exception.Message)" "ERROR"
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
        Write-Log "Ошибка при запуске удаленного сервиса $($Service.Name): $($_.Exception.Message)" "ERROR"
        return $false
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
            }
        }
        
        return $true
    }
    catch {
        Write-Log "Ошибка при очистке папки сервиса $($Service.Name): $($_.Exception.Message)" "WARNING"
        return $false
    }
}

# Основная логика
Write-Log "=== Удаленное развертывание Backend-сервисов ==="

# Загружаем конфигурацию
$config = Get-Config -ConfigPath $ConfigPath
Write-Log "Конфигурация загружена из: $ConfigPath"

# Проверяем подключение к серверам
if (-not (Test-NetworkConnection -Hostname $config.DevServer.Hostname -IP $config.DevServer.IP)) {
    exit 1
}

if (-not (Test-NetworkConnection -Hostname $config.ProdServer.Hostname -IP $config.ProdServer.IP)) {
    exit 1
}

# Определяем что делать
$doBuild = $BuildOnly -or (-not $DeployOnly -and -not $ClearOnly -and -not $ManageServices)
$doDeploy = $DeployOnly -or (-not $BuildOnly -and -not $ClearOnly -and -not $ManageServices)
$doClear = $ClearOnly -or (-not $BuildOnly -and -not $DeployOnly -and -not $ManageServices)
$doManageServices = $ManageServices

$successCount = 0
$totalCount = $config.BackendServices.Count

# Обрабатываем сервисы
foreach ($service in $config.BackendServices) {
    Write-Log "Обрабатываю сервис: $($service.Name) (порт: $($service.Port))"
    
    $serviceSuccess = $true
    
    if ($doBuild) {
        if (-not (Build-RemoteService -Service $service -DevServer $config.DevServer)) {
            $serviceSuccess = $false
        }
    }
    
    if ($doDeploy -and $serviceSuccess) {
        if (-not (Stop-RemoteProcessByPort -Port $service.Port -Server $config.ProdServer)) {
            Write-Log "Предупреждение: не удалось остановить сервис $($service.Name)" "WARNING"
        }
        
        if (-not $NoClear) {
            Clear-RemoteServiceFolder -Service $service -ProdServer $config.ProdServer | Out-Null
        }
        
        if (-not (Copy-RemoteServiceSMB -Service $service -DevServer $config.DevServer -ProdServer $config.ProdServer)) {
            $serviceSuccess = $false
        }
        
        if ($serviceSuccess -and -not (Start-RemoteService -Service $service -ProdServer $config.ProdServer)) {
            $serviceSuccess = $false
        }
    }
    
    if ($doClear) {
        Clear-RemoteServiceFolder -Service $service -ProdServer $config.ProdServer | Out-Null
    }
    
    if ($serviceSuccess) {
        $successCount++
    }
}

Write-Log "=== Операция завершена ==="
Write-Log "Обработано сервисов: $successCount из $totalCount"

if ($successCount -eq $totalCount) {
    Write-Log "Все сервисы обработаны успешно!" "INFO"
} else {
    Write-Log "Обработано $successCount из $totalCount сервисов" "WARNING"
}
