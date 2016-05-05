@ECHO OFF

REM -------------------------------------------------------------------
REM   Set the global variables for the batch file script
REM       - ConfigName: the name of the configuration folder and the
REM                     bat file used to implement the configuration
REM                     for a given dataset
REM       - DatasetName: the name of the dataset folder in the data
REM                      path (note: there can be more than one
REM                      configuration for a single dataset)
REM -------------------------------------------------------------------
SET ConfigName=%~n0
SET DatasetName=dataexplorers
SET DataPath=D:\ETL\Sisula
SET SisulaPath=N:\My Documents\Sisula\sisula-ETL-0.0.2
SET SisulaPathDrive=%SisulaPath:~0,2%
SET ConfigPath=%SisulaPath%\config
SET DirectivePath=%SisulaPath%\directive
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
  ECHO  * ERROR: Missing drive
  ECHO  * Terminating batch script
  EXIT /B 1
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
) ELSE (
  ECHO  * Missing Sisula configuration directory
  ECHO  * ERROR: Missing directory
  ECHO  * Terminating batch script
  EXIT /B 1
)
ECHO  * Checking data directory path structure
IF EXIST "%DataPath%\%DatasetName%" (
  ECHO  * Data directory path found:
  ECHO      %DataPath%\%DatasetName%
) ELSE (
  ECHO  * WARNING: Missing data directory path structure
  ECHO  * Creating data directories:
  ECHO      %DataPath%\%DatasetName%\log ...
  ECHO      %DataPath%\%DatasetName%\temp ...
  ECHO      %DataPath%\%DatasetName%\data ...
  ECHO      %DataPath%\%DatasetName%\data\syncing ...
  ECHO      %DataPath%\%DatasetName%\data\processing ...
  ECHO      %DataPath%\%DatasetName%\data\incoming ...
  ECHO      %DataPath%\%DatasetName%\data\working ...
  ECHO      %DataPath%\%DatasetName%\data\archive
  MKDIR "%DataPath%\%DatasetName%\log"
  MKDIR "%DataPath%\%DatasetName%\temp"
  MKDIR "%DataPath%\%DatasetName%\data"
  MKDIR "%DataPath%\%DatasetName%\data\syncing"
  MKDIR "%DataPath%\%DatasetName%\data\processing"
  MKDIR "%DataPath%\%DatasetName%\data\incoming"
  MKDIR "%DataPath%\%DatasetName%\data\working"
  MKDIR "%DataPath%\%DatasetName%\data\archive"
)

REM -------------------------------------------------------------------
REM   Call Sisulate.bat with the required configuration
REM   name and the optional SQL Server name.
REM -------------------------------------------------------------------
ECHO  * Calling batch script with arguments:
ECHO      %SisulaPath%\Sisulate.bat "%ConfigName%" %1
ECHO.
CALL "%SisulaPath%\Sisulate.bat" "%ConfigName%" %1
