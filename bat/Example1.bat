@ECHO OFF
SET SisulaPath=N:\My Documents\Sisula\sisula-ETL-0.0.2
SET SisulaPathDrive=%SisulaPath:~0,2%
SET ConfigPath=%SisulaPath%\config
SET ConfigName=%~n0
SET DirectivePath=%SisulaPath%\directive
SET DataPath=D:\Sisula ETL
SET DataPathDrive=%DataPath:~0,2%

ECHO -------------------------------------------------------------------
ECHO  Running configuration batch script     %date% %time%
ECHO -------------------------------------------------------------------
ECHO.
ECHO  * Batch script file name:
ECHO      %~nx0
ECHO  * Sisula directory path:
ECHO      %SisulaPath%
ECHO  * Sisula directory path drive:
ECHO      %SisulaPathDrive%
ECHO  * Configuration directory path:
ECHO      %ConfigPath%
ECHO  * Configuration directory name:
ECHO      %ConfigName%
ECHO  * Directive directory path:
ECHO      %DirectivePath%
ECHO  * Sisula data directory path:
ECHO      %DataPath%
ECHO  * Sisula data directory path drive:
ECHO      %DataPathDrive%

REM -------------------------------------------------------------------
REM   Check drives, directories, and paths. Create directories and
REM   paths if missing. Abort if drive is missing.
REM -------------------------------------------------------------------
ECHO  * Checking drives
IF EXIST "%SisulaPathDrive%" (
  ECHO  * Found Sisula directory path drive:
  ECHO      %SisulaPathDrive%
) ELSE (
  ECHO  * Missing Sisula directory path drive
  GOTO ERROR
)
IF EXIST "%DataPathDrive%" (
  ECHO  * Found Sisula data directory path drive:
  ECHO      %DataPathDrive%
) ELSE (
  ECHO  * Missing Sisula data directory path drive
  ECHO  * ERROR: Missing drive
  ECHO  * Terminating batch script
  EXIT /B 1
)
ECHO  * Checking configuration path directory
IF EXIST "%ConfigPath%\%ConfigName%" (
  ECHO  * Configuration directory found:
  ECHO      %ConfigPath%\%ConfigName%
  ECHO  * Checking data directory path structure
  IF EXIST "%DataPath%\%ConfigName%" (
    ECHO  * Data directory path found:
    ECHO      %DataPath%\%ConfigName%
  ) ELSE (
    ECHO  * WARNING: Missing data directory path structure
    ECHO  * Creating data directories:
    ECHO      %DataPath%\%ConfigName%\incoming ...
    ECHO      %DataPath%\%ConfigName%\work ...
    ECHO      %DataPath%\%ConfigName%\archive
    MKDIR "%DataPath%\%ConfigName%\incoming"
    MKDIR "%DataPath%\%ConfigName%\work"
    MKDIR "%DataPath%\%ConfigName%\archive"
  )
)

REM -------------------------------------------------------------------
REM   Call Sisulate.bat with the required configuration
REM   name and the optional SQL Server name.
REM -------------------------------------------------------------------
ECHO  * Calling batch script with arguments:
ECHO      %SisulaPath%\Sisulate.bat "%ConfigName%" %1
ECHO.
CALL "%SisulaPath%\Sisulate.bat" "%ConfigName%" %1
