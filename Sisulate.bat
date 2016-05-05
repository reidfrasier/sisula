@ECHO OFF

REM -------------------------------------------------------------------
REM   Set the global variables for the batch file script
REM   TODO: the folder name variable is redundant with config name
REM   defined in the bat file that calls this script. Either change the
REM   variable name to config name and delete the set statement or
REM   reorganize the variable declarations to be more consistent
REM -------------------------------------------------------------------
SET FolderName=%~1
SET FolderPath=%ConfigPath%\%FolderName%
SET SqlServer=%2
SET SqlFiles=

REM -------------------------------------------------------------------
REM   Print out program syntax if no arguments are given
REM -------------------------------------------------------------------
IF "%FolderName%"=="" (
  ECHO -----------------------------------------------------------------------
  ECHO  ERROR: Missing configuration name argument. Please ensure that the
  ECHO         batch script is called with the correct syntax.
  ECHO.
  ECHO  The syntax should have the form:
  ECHO.
  ECHO      Sisulate.bat ^<configuration name^> [SQL server name]
  ECHO.
  ECHO    with arguments:
  ECHO.
  ECHO      ^<configuration name^>  String representing a directory name in
  ECHO                            ^<Sisula installation path^>\config.
  ECHO.
  ECHO      [SQL server name]     ^(Optional^) String representing the name
  ECHO                            of the server in which to execute the
  ECHO                            generated SQL scripts using Sqlcmd for
  ECHO                            Microsoft SQL Server.
  ECHO -----------------------------------------------------------------------
  GOTO ERROR
)

REM -------------------------------------------------------------------
REM   Print out error message for incorrect arguments
REM -------------------------------------------------------------------
IF NOT EXIST "%FolderPath%" (
  ECHO -----------------------------------------------------------------------
  ECHO  ERROR: Incorrect configuration name. Please ensure that the name
  ECHO  of the batch script file is the same as the name of the directory
  ECHO  containing the configuration files.
  ECHO.
  ECHO  The configuration directory path should have the form:
  ECHO    ^<Sisula installation path^>\config\^<configuration name^>
  ECHO.
  ECHO  The batch script file should have the following directory path
  ECHO  and form:
  ECHO    ^<Sisula installation path^>\bat\^<configuration name^>.bat
  ECHO -----------------------------------------------------------------------
  GOTO ERROR
)

ECHO -------------------------------------------------------------------
ECHO  Sisula starting                        %date% %time%
ECHO -------------------------------------------------------------------
ECHO.

REM -------------------------------------------------------------------
REM   This file needs to be saved as UTF-8 with the option "No Mark"
REM -------------------------------------------------------------------
FOR /F "tokens=2 delims=:." %%x IN ('CHCP') DO SET DEFAULT_CODEPAGE=%%x
CHCP 65001>NUL
ECHO  * Path to the Sisula ETL Framework installation:
ECHO      %SisulaPath%
ECHO  * Path to the specified folder containing configuration files:
ECHO      %FolderPath%
PUSHD "%SisulaPath%"
ECHO  * Entered PUSHD directory:
ECHO      %CD%

REM -------------------------------------------------------------------
REM   Initiate project specific variables
REM -------------------------------------------------------------------
CALL "%FolderPath%\Variables.bat"

SETLOCAL ENABLEDELAYEDEXPANSION ENABLEEXTENSIONS
SET i=-1

REM -------------------------------------------------------------------
REM   Create bulk format files
REM -------------------------------------------------------------------
FOR %%f IN ("%FolderPath%\sources\*.xml") DO (
  SET OutputFile=%FolderPath%\formats\%%~nf.xml
  ECHO  * Transforming source to bulk format file:
  ECHO      %%~f ...
  ECHO      !OutputFile!
  Sisulator.js -x "%%~f" -m Source -d "%DirectivePath%\format.directive" -o "!OutputFile!"
  IF ERRORLEVEL 1 GOTO ERROR
)

REM -------------------------------------------------------------------
REM   Create source loading SQL code
REM   Note that the name of the corresponding format file is needed
REM -------------------------------------------------------------------
FOR %%f IN ("%FolderPath%\sources\*.xml") DO (
  SET OutputFile=%FolderPath%\sources\%%~nf.sql
REM   The below line has been commented out as unnecessary because the
REM   format files are created in the previous step and the files are
REM   used in the SQL Server by the BULK INSERT statement only when
REM   "bulk" is specified as the desired split type as opposed to "regex".
REM  SET FormatFile=%FolderPath%\formats\%%~nf.xml
  ECHO  * Transforming source to SQL loading stored procedures:
  ECHO      %%~f ...
  ECHO      !OutputFile!
  Sisulator.js -x "%%~f" -m Source -d "%DirectivePath%\source.directive" -o "!OutputFile!"
  IF ERRORLEVEL 1 GOTO ERROR
  SET /A i=!i!+1
  SET SqlFiles[!i!]=!OutputFile!
)

REM -------------------------------------------------------------------
REM   Create target loading SQL code
REM -------------------------------------------------------------------
FOR %%f IN ("%FolderPath%\targets\*.xml") DO (
  SET OutputFile=%FolderPath%\targets\%%~nf.sql
  ECHO  * Transforming target to SQL loading stored procedures:
  ECHO      %%~f ...
  ECHO      !OutputFile!
  Sisulator.js -x "%%~f" -m Target -d "%DirectivePath%\target.directive" -o "!OutputFile!"
  IF ERRORLEVEL 1 GOTO ERROR
  SET /A i=!i!+1
  SET SqlFiles[!i!]=!OutputFile!
)

REM -------------------------------------------------------------------
REM   Create SQL Server Agent job code
REM -------------------------------------------------------------------
FOR %%f IN ("%FolderPath%\workflows\*.xml") DO (
  SET OutputFile=%FolderPath%\workflows\%%~nf.sql
  ECHO  * Transforming workflow to SQL Server Agent job scripts:
  ECHO      %%~f ...
  ECHO      !OutputFile!
  Sisulator.js -x "%%~f" -m Workflow -d "%DirectivePath%\workflow.directive" -o "!OutputFile!"
  IF ERRORLEVEL 1 GOTO ERROR
  SET /A i=!i!+1
  SET SqlFiles[!i!]=!OutputFile!
)

REM -------------------------------------------------------------------
REM   Install the generated SQL files in the database server
REM -------------------------------------------------------------------
IF DEFINED SqlServer (
  FOR /L %%f IN (0,1,!i!) DO (
    ECHO.
    ECHO  * Installing SQL file:
    ECHO      !SqlFiles[%%f]!
    ECHO.
    Sqlcmd -S %SqlServer% -i "!SqlFiles[%%f]!" -I -x -b -r1 >NUL
    IF ERRORLEVEL 1 GOTO ERROR
  )
)

ECHO.
ECHO -------------------------------------------------------------------
ECHO  Sisula ending                          %date% %time%
ECHO -------------------------------------------------------------------

:ERROR
REM -------------------------------------------------------------------
REM This is the end (do these things regardless of state)
REM -------------------------------------------------------------------
ENDLOCAL
CHCP %DEFAULT_CODEPAGE%>NUL
POPD
