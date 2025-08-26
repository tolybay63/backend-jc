@echo off
echo === Services Status Check ===

set PROD_SERVER=192.168.1.20

echo Setting up network connection...
net use \\%PROD_SERVER%\C$ /user:%USERNAME% 2>nul

echo Creating simple status check script on server...
(
echo @echo off
echo echo === Services Status Check ===
echo echo.
echo echo Checking our services status:
echo echo.
echo set /p=Admin Service ^(Port 9172^): ^<nul
echo netstat -ano ^| findstr ":9172" ^>nul ^&^& echo [RUNNING] ^|^| echo [STOPPED]
echo set /p=Meta Service ^(Port 9173^): ^<nul
echo netstat -ano ^| findstr ":9173" ^>nul ^&^& echo [RUNNING] ^|^| echo [STOPPED]
echo set /p=Account Service ^(Port 9174^): ^<nul
echo netstat -ano ^| findstr ":9174" ^>nul ^&^& echo [RUNNING] ^|^| echo [STOPPED]
echo set /p=Tofidata Service ^(Port 9175^): ^<nul
echo netstat -ano ^| findstr ":9175" ^>nul ^&^& echo [RUNNING] ^|^| echo [STOPPED]
echo set /p=Cube Service ^(Port 9176^): ^<nul
echo netstat -ano ^| findstr ":9176" ^>nul ^&^& echo [RUNNING] ^|^| echo [STOPPED]
echo set /p=NSI Service ^(Port 9177^): ^<nul
echo netstat -ano ^| findstr ":9177" ^>nul ^&^& echo [RUNNING] ^|^| echo [STOPPED]
echo set /p=Object Service ^(Port 9178^): ^<nul
echo netstat -ano ^| findstr ":9178" ^>nul ^&^& echo [RUNNING] ^|^| echo [STOPPED]
echo set /p=Plan Service ^(Port 9179^): ^<nul
echo netstat -ano ^| findstr ":9179" ^>nul ^&^& echo [RUNNING] ^|^| echo [STOPPED]
echo set /p=Personnal Service ^(Port 9180^): ^<nul
echo netstat -ano ^| findstr ":9180" ^>nul ^&^& echo [RUNNING] ^|^| echo [STOPPED]
echo set /p=OrgStructure Service ^(Port 9181^): ^<nul
echo netstat -ano ^| findstr ":9181" ^>nul ^&^& echo [RUNNING] ^|^| echo [STOPPED]
echo set /p=Inspection Service ^(Port 9182^): ^<nul
echo netstat -ano ^| findstr ":9182" ^>nul ^&^& echo [RUNNING] ^|^| echo [STOPPED]
echo echo.
echo echo === Status check completed ===
echo pause
) > "\\%PROD_SERVER%\C$\check_status_simple.bat"

echo Executing status check on server...
psexec \\%PROD_SERVER% cmd /c "C:\check_status_simple.bat"

echo === Status check completed ===
pause
