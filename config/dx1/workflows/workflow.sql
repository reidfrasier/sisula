------------------------------- WQ_Dataexplorers_Workflow -------------------------------
USE msdb;
GO
IF EXISTS (select job_id from [dbo].[sysjobs_view] where name = 'WQ_Dataexplorers_Extracting')
EXEC sp_delete_job
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Extracting';
GO
sp_add_job
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Extracting';
GO
sp_add_jobserver
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Extracting';
GO
sp_add_jobstep
    @job_name = 'WQ_Dataexplorers_Extracting',
    @step_name = 'Log starting of job',
    @step_id = 1,
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStarting @workflowName = ''WQ_Dataexplorers_Workflow'', @jobName = ''WQ_Dataexplorers_Extracting'', @agentJobId = $(ESCAPE_NONE(JOBID))',
    @on_success_action = 3; -- go to the next step
GO
sp_add_jobstep
    @subsystem = 'PowerShell',
    @command = '
            $PowerShellVersion4 = PowerShell {
              # set session environment variables
              $env:PSModulePath = $env:PSModulePath + ";C:\Users\rfrasier\Documents\WindowsPowerShell\Modules;"
              # import modules
              Import-Module -Name "Credentials" -RequiredVersion "1.0"
              Import-Module -Name "WinSCP" -RequiredVersion "5.8.2.0"
              Import-Module -Name "WinZip" -RequiredVersion "1.0"
              Import-Module -Name "Winix" -RequiredVersion "1.0"
              # get credential
              $credential = Import-Credential -Path "D:\Credentials\rfrasier.xml"
              # connect to remote server
              $session = New-WinSCPSession -Protocol "Sftp" -HostName "wqnfs4tk1.mlp.com" -Credential $credential -GiveUpSecurityAndAcceptAnySshHostKey
              # set session timeout
              $session.Timeout = New-TimeSpan -Minutes 20
              # initialize job step object
              $jobStep = New-Object -TypeName "System.Management.Automation.PSObject"
              $jobStep | Add-Member -MemberType "NoteProperty" -Name "ResultsType" -Value "Sync"
              $jobStep | Add-Member -MemberType "NoteProperty" -Name "StartTime" -Value (Get-Date)
              $jobStep | Add-Member -MemberType "NoteProperty" -Name "EndTime" -Value $null
              $jobStep | Add-Member -MemberType "NoteProperty" -Name "IsSuccess" -Value $false
              $jobStep | Add-Member -MemberType "NoteProperty" -Name "Results" -Value $null
              try {
                # start sync
                $syncResults = Sync-WinixParsedPath -WinSCPSession $session -Mode "Local" -Criteria "Time" -RemotePath "/dat/septa/theatrain/dataexplorers" -LocalPath "D:\ETL\Sisula\dataexplorers\data\syncing" -RemoteRootPath "/dat/septa/theatrain/dataexplorers" -LocalRootPath "D:\ETL\Sisula\dataexplorers\data\syncing"
                # set properties
                $jobStep.EndTime = (Get-Date)
                $jobStep.IsSuccess = $true
                $jobStep.Results = $syncResults
              }
              catch {
                # write error
                Write-Error -Message "Sync failed"
                # set properties
                $jobStep.EndTime = (Get-Date)
                $jobStep.IsSuccess = $false
              }
              # set xml file path
              $xmlFilePath = Join-Path -Path "D:\ETL\Sisula\dataexplorers\temp" -ChildPath "WQ_Dataexplorers_Extracting.xml"
              # export job step object
              Export-Clixml -InputObject $jobStep -LiteralPath $xmlFilePath -Force
              # send notification email
              # disconnect from remote server
              Remove-WinSCPSession -WinSCPSession $session
            }
        ',
    @proxy_name = 'RFrasier',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Extracting',
    @step_name = 'Sync local files with remote server';
GO
sp_add_jobstep
    @subsystem = 'PowerShell',
    @command = '
            $PowerShellVersion4 = PowerShell {
              # get xml file path
              $xmlFilePath = Join-Path -Path "D:\ETL\Sisula\dataexplorers\temp" -ChildPath "WQ_Dataexplorers_Extracting.xml"
              # check if xml file exists
              if (Test-Path -Path $xmlFilePath -PathType "Leaf") {
                # import job step object
                $jobStepInfo = Import-Clixml -LiteralPath $xmlFilePath
              }
              else {
                Throw "Sync job step xml file $($xmlFilePath) does not exist."
              }
              # check if prior job step did not succeed
              if (-not $jobStepInfo.IsSuccess) {
                Throw "Prior sync job step did not succeed. It must be completed successfully before the workflow can continue."
              }
              # get prior job step results
              $jobStepResults = $jobStepInfo.Results
              # iterate through job step results
              foreach ($syncResult in $syncResults) {
                # check if file was synced
                if ($syncResult.IsSuccess) {
                }
              }
            }
        ',
    @proxy_name = 'RFrasier',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Extracting',
    @step_name = 'Process synced files';
GO
sp_add_jobstep
    @job_name = 'WQ_Dataexplorers_Extracting',
    @step_name = 'Log success of job',
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStopping @name = ''WQ_Dataexplorers_Extracting'', @status = ''Success''',
    @on_success_action = 1; -- quit with success
GO
sp_add_jobstep
    @job_name = 'WQ_Dataexplorers_Extracting',
    @step_name = 'Log failure of job',
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStopping @name = ''WQ_Dataexplorers_Extracting'', @status = ''Failure''',
    @on_success_action = 2; -- quit with failure
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Extracting',
    @step_id = 2,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 5;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Extracting',
    @step_id = 3,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 5;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Extracting',
    @step_id = 3,
    -- ensure logging when last step succeeds
    @on_success_action = 4, -- go to step with id
    @on_success_step_id = 4;
GO
IF EXISTS (select job_id from [dbo].[sysjobs_view] where name = 'WQ_Dataexplorers_Staging')
EXEC sp_delete_job
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Staging';
GO
sp_add_job
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Staging';
GO
sp_add_jobserver
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Staging';
GO
sp_add_jobstep
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_name = 'Log starting of job',
    @step_id = 1,
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStarting @workflowName = ''WQ_Dataexplorers_Workflow'', @jobName = ''WQ_Dataexplorers_Staging'', @agentJobId = $(ESCAPE_NONE(JOBID))',
    @on_success_action = 3; -- go to the next step
GO
sp_add_jobstep
    @subsystem = 'PowerShell',
    @command = '
            $PowerShellVersion4 = PowerShell {
              $files = Get-ChildItem -Path "D:\ETL\Sisula\dataexplorers\data\incoming" | Where-Object {$_.Name -match "*"}
              If ($files.Length -eq 0) {
                Throw "No matching files were found in D:\ETL\Sisula\dataexplorers\data\incoming"
              }
              Else {
                ForEach ($file in $files) {
                  $fullFileName = $file.FullName
                  Move-Item -Path $fullFileName -Destination "D:\ETL\Sisula\dataexplorers\data\working" -Force
                  Write-Output "Moved file: $fullFileName to D:\ETL\Sisula\dataexplorers\data\working"
                }
              }
            }
        ',
    @proxy_name = 'RFrasier',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_name = 'Check for and move files';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.WQ_Dataexplorers_CreateRawTable @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_name = 'Create raw table';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.WQ_Dataexplorers_CreateInsertView @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_name = 'Create insert view';
GO
sp_add_jobstep
    @subsystem = 'PowerShell',
    @command = '
            $PowerShellVersion4 = PowerShell {
              $files = Get-ChildItem -Path "D:\ETL\Sisula\dataexplorers\data\working" -Recurse | Where-Object {$_.Name -match "*"}
              If ($files.Length -eq 0) {
                Throw "No matching files were found in D:\ETL\Sisula\dataexplorers\data\working"
              }
              Else {
                ForEach ($file in $files) {
                  $fullFileName = $file.FullName
                  $modifiedDate = $file.LastWriteTime
                  Invoke-Sqlcmd "EXEC source.WQ_Dataexplorers_BulkInsert `"$fullFileName`", `"$modifiedDate`", @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))" -Database "Development" -ErrorAction Stop -QueryTimeout 0
                  Write-Output "Loaded file: $fullFileName"
                  Move-Item -Path $fullFileName -Destination "D:\ETL\Sisula\dataexplorers\data\archive" -Force
                  Write-Output "Moved file: $fullFileName to D:\ETL\Sisula\dataexplorers\data\archive"
                }
              }
            }
        ',
    @database_name = 'Development',
    @proxy_name = 'RFrasier',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_name = 'Bulk insert';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.WQ_Dataexplorers_CreateSplitViews @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_name = 'Create split views';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.WQ_Dataexplorers_CreateErrorViews @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_name = 'Create error views';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.WQ_Dataexplorers_CreateTypedTables @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_name = 'Create typed tables';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.WQ_Dataexplorers_SplitRawIntoTyped @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_name = 'Split raw into typed';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.WQ_Dataexplorers_AddKeysToTyped @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_name = 'Add keys to typed';
GO
sp_add_jobstep
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_name = 'Log success of job',
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStopping @name = ''WQ_Dataexplorers_Staging'', @status = ''Success''',
    @on_success_action = 1; -- quit with success
GO
sp_add_jobstep
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_name = 'Log failure of job',
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStopping @name = ''WQ_Dataexplorers_Staging'', @status = ''Failure''',
    @on_success_action = 2; -- quit with failure
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_id = 2,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_id = 3,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_id = 4,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_id = 5,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_id = 6,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_id = 7,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_id = 8,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_id = 9,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_id = 10,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Staging',
    @step_id = 10,
    -- ensure logging when last step succeeds
    @on_success_action = 4, -- go to step with id
    @on_success_step_id = 11;
GO
IF EXISTS (select job_id from [dbo].[sysjobs_view] where name = 'WQ_Dataexplorers_Loading')
EXEC sp_delete_job
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Loading';
GO
sp_add_job
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Loading';
GO
sp_add_jobserver
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Loading';
GO
sp_add_jobstep
    @job_name = 'WQ_Dataexplorers_Loading',
    @step_name = 'Log starting of job',
    @step_id = 1,
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStarting @workflowName = ''WQ_Dataexplorers_Workflow'', @jobName = ''WQ_Dataexplorers_Loading'', @agentJobId = $(ESCAPE_NONE(JOBID))',
    @on_success_action = 3; -- go to the next step
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.[lST_Street__WQ_Dataexplorers_Collision_Typed] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Loading',
    @step_name = 'Load streets';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.[lIS_Intersection__WQ_Dataexplorers_Collision_Typed__1] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Loading',
    @step_name = 'Load intersection pass 1';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.[lST_intersecting_IS_of_ST_crossing__WQ_Dataexplorers_Collision_Typed] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Loading',
    @step_name = 'Load ST ST IS tie';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.[lIS_Intersection__WQ_Dataexplorers_Collision_Typed__2] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    -- mandatory parameters below and optional ones above this line
    @job_name = 'WQ_Dataexplorers_Loading',
    @step_name = 'Load intersection pass 2';
GO
sp_add_jobstep
    @job_name = 'WQ_Dataexplorers_Loading',
    @step_name = 'Log success of job',
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStopping @name = ''WQ_Dataexplorers_Loading'', @status = ''Success''',
    @on_success_action = 1; -- quit with success
GO
sp_add_jobstep
    @job_name = 'WQ_Dataexplorers_Loading',
    @step_name = 'Log failure of job',
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStopping @name = ''WQ_Dataexplorers_Loading'', @status = ''Failure''',
    @on_success_action = 2; -- quit with failure
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Loading',
    @step_id = 2,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 7;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Loading',
    @step_id = 3,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 7;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Loading',
    @step_id = 4,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 7;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Loading',
    @step_id = 5,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 7;
GO
sp_update_jobstep
    @job_name = 'WQ_Dataexplorers_Loading',
    @step_id = 5,
    -- ensure logging when last step succeeds
    @on_success_action = 4, -- go to step with id
    @on_success_step_id = 6;
GO
-- The workflow definition used when generating the above
DECLARE @xml XML = N'<workflow name="WQ_Dataexplorers_Workflow">
	<variable name="remoteFilePathRegexPattern" value="*"/>
	<variable name="remoteDatasetRootPath" value="/dat/septa/theatrain/dataexplorers"/>
	<variable name="credentialsRootPath" value="D:\Credentials"/>
	<variable name="localDatasetRootPath" value="D:\ETL\Sisula\dataexplorers"/>
	<variable name="logPath" value="D:\ETL\Sisula\dataexplorers\log"/>
	<variable name="tempPath" value="D:\ETL\Sisula\dataexplorers\temp"/>
	<variable name="syncingPath" value="D:\ETL\Sisula\dataexplorers\data\syncing"/>
	<variable name="processingPath" value="D:\ETL\Sisula\dataexplorers\data\processing"/>
	<variable name="incomingPath" value="D:\ETL\Sisula\dataexplorers\data\incoming"/>
	<variable name="workingPath" value="D:\ETL\Sisula\dataexplorers\data\working"/>
	<variable name="archivePath" value="D:\ETL\Sisula\dataexplorers\data\archive"/>
	<variable name="emailToNameList" value="Reid Frasier"/>
	<variable name="emailToAddressList" value="Reid.Frasier@mlp.com"/>
	<variable name="extraOptions" value="-Recurse"/>
	<variable name="addToPowerShellModulePath" value=";C:\Users\rfrasier\Documents\WindowsPowerShell\Modules;"/>
	<variable name="credentialsModule" value="Credentials"/>
	<variable name="credentialsModuleVersion" value="1.0"/>
	<variable name="winSCPModule" value="WinSCP"/>
	<variable name="winSCPModuleVersion" value="5.8.2.0"/>
	<variable name="winixModule" value="WinZip"/>
	<variable name="winixModuleVersion" value="1.0"/>
	<variable name="winZipModule" value="Winix"/>
	<variable name="winZipModuleVersion" value="1.0"/>
	<variable name="powerShellProxyName" value="RFrasier"/>
	<variable name="startPowerShellSession" value="$PowerShellVersion4 = PowerShell {"/>
	<variable name="endPowerShellSession" value="}"/>
	<variable name="quitWithSuccess" value="1"/>
	<variable name="quitWithFailure" value="2"/>
	<variable name="goToTheNextStep" value="3"/>
	<variable name="goToStepWithId" value="4"/>
	<variable name="queryTimeout" value="0"/>
	<variable name="parameters" value="@agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))"/>
	<job name="WQ_Dataexplorers_Extracting">
		<variable name="credentialsFileName" value="rfrasier.xml"/>
		<variable name="remoteServerHostName" value="wqnfs4tk1.mlp.com"/>
		<variable name="winScpProtocol" value="Sftp"/>
		<variable name="winScpSessionTimeout" value="20"/>
		<variable name="winScpSyncMode" value="Local"/>
		<variable name="winScpSyncCriteria" value="Time"/>
		<jobstep name="Sync local files with remote server" subsystem="PowerShell" proxy_name="RFrasier" on_success_action="3">
            $PowerShellVersion4 = PowerShell {
              # set session environment variables
              $env:PSModulePath = $env:PSModulePath + ";C:\Users\rfrasier\Documents\WindowsPowerShell\Modules;"
              # import modules
              Import-Module -Name "Credentials" -RequiredVersion "1.0"
              Import-Module -Name "WinSCP" -RequiredVersion "5.8.2.0"
              Import-Module -Name "WinZip" -RequiredVersion "1.0"
              Import-Module -Name "Winix" -RequiredVersion "1.0"
              # get credential
              $credential = Import-Credential -Path "%DataPathDrive%\Credentials\%credentialsFileName%"
              # connect to remote server
              $session = New-WinSCPSession -Protocol "%winScpProtocol%" -HostName "%remoteServerHostName%" -Credential $credential -GiveUpSecurityAndAcceptAnySshHostKey
              # set session timeout
              $session.Timeout = New-TimeSpan -Minutes %winScpSessionTimeout%
              # initialize job step object
              $jobStep = New-Object -TypeName "System.Management.Automation.PSObject"
              $jobStep | Add-Member -MemberType "NoteProperty" -Name "ResultsType" -Value "Sync"
              $jobStep | Add-Member -MemberType "NoteProperty" -Name "StartTime" -Value (Get-Date)
              $jobStep | Add-Member -MemberType "NoteProperty" -Name "EndTime" -Value $null
              $jobStep | Add-Member -MemberType "NoteProperty" -Name "IsSuccess" -Value $false
              $jobStep | Add-Member -MemberType "NoteProperty" -Name "Results" -Value $null
              try {
                # start sync
                $syncResults = Sync-WinixParsedPath -WinSCPSession $session -Mode "%winScpSyncMode%" -Criteria "%winScpSyncCriteria%" -RemotePath "/dat/septa/theatrain/dataexplorers" -LocalPath "%DataPath%\%DatasetName%\data\syncing" -RemoteRootPath "/dat/septa/theatrain/dataexplorers" -LocalRootPath "%DataPath%\%DatasetName%\data\syncing"
                # set properties
                $jobStep.EndTime = (Get-Date)
                $jobStep.IsSuccess = $true
                $jobStep.Results = $syncResults
              }
              catch {
                # write error
                Write-Error -Message "Sync failed"
                # set properties
                $jobStep.EndTime = (Get-Date)
                $jobStep.IsSuccess = $false
              }
              # set xml file path
              $xmlFilePath = Join-Path -Path "%DataPath%\%DatasetName%\temp" -ChildPath "WQ_Dataexplorers_Extracting.xml"
              # export job step object
              Export-Clixml -InputObject $jobStep -LiteralPath $xmlFilePath -Force
              # send notification email
              # disconnect from remote server
              Remove-WinSCPSession -WinSCPSession $session
            }
        </jobstep>
		<jobstep name="Process synced files" subsystem="PowerShell" proxy_name="RFrasier" on_success_action="3">
            $PowerShellVersion4 = PowerShell {
              # get xml file path
              $xmlFilePath = Join-Path -Path "%DataPath%\%DatasetName%\temp" -ChildPath "WQ_Dataexplorers_Extracting.xml"
              # check if xml file exists
              if (Test-Path -Path $xmlFilePath -PathType "Leaf") {
                # import job step object
                $jobStepInfo = Import-Clixml -LiteralPath $xmlFilePath
              }
              else {
                Throw "Sync job step xml file $($xmlFilePath) does not exist."
              }
              # check if prior job step did not succeed
              if (-not $jobStepInfo.IsSuccess) {
                Throw "Prior sync job step did not succeed. It must be completed successfully before the workflow can continue."
              }
              # get prior job step results
              $jobStepResults = $jobStepInfo.Results
              # iterate through job step results
              foreach ($syncResult in $syncResults) {
                # check if file was synced
                if ($syncResult.IsSuccess) {
                }
              }
            }
        </jobstep>
	</job>
	<job name="WQ_Dataexplorers_Staging">
		<jobstep name="Check for and move files" subsystem="PowerShell" proxy_name="RFrasier" on_success_action="3">
            $PowerShellVersion4 = PowerShell {
              $files = Get-ChildItem -Path "%DataPath%\%DatasetName%\data\incoming" | Where-Object {$_.Name -match "*"}
              If ($files.Length -eq 0) {
                Throw "No matching files were found in %DataPath%\%DatasetName%\data\incoming"
              }
              Else {
                ForEach ($file in $files) {
                  $fullFileName = $file.FullName
                  Move-Item -Path $fullFileName -Destination "%DataPath%\%DatasetName%\data\working" -Force
                  Write-Output "Moved file: $fullFileName to %DataPath%\%DatasetName%\data\working"
                }
              }
            }
        </jobstep>
		<jobstep name="Create raw table" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.WQ_Dataexplorers_CreateRawTable @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Create insert view" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.WQ_Dataexplorers_CreateInsertView @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Bulk insert" database_name="Development" subsystem="PowerShell" proxy_name="RFrasier" on_success_action="3">
            $PowerShellVersion4 = PowerShell {
              $files = Get-ChildItem -Path "%DataPath%\%DatasetName%\data\working" -Recurse | Where-Object {$_.Name -match "*"}
              If ($files.Length -eq 0) {
                Throw "No matching files were found in %DataPath%\%DatasetName%\data\working"
              }
              Else {
                ForEach ($file in $files) {
                  $fullFileName = $file.FullName
                  $modifiedDate = $file.LastWriteTime
                  Invoke-Sqlcmd "EXEC source.WQ_Dataexplorers_BulkInsert `"$fullFileName`", `"$modifiedDate`", @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))" -Database "Development" -ErrorAction Stop -QueryTimeout 0
                  Write-Output "Loaded file: $fullFileName"
                  Move-Item -Path $fullFileName -Destination "%DataPath%\%DatasetName%\data\archive" -Force
                  Write-Output "Moved file: $fullFileName to %DataPath%\%DatasetName%\data\archive"
                }
              }
            }
        </jobstep>
		<jobstep name="Create split views" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.WQ_Dataexplorers_CreateSplitViews @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Create error views" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.WQ_Dataexplorers_CreateErrorViews @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Create typed tables" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.WQ_Dataexplorers_CreateTypedTables @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Split raw into typed" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.WQ_Dataexplorers_SplitRawIntoTyped @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Add keys to typed" database_name="Development" subsystem="TSQL">
            EXEC source.WQ_Dataexplorers_AddKeysToTyped @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
	</job>
	<job name="WQ_Dataexplorers_Loading">
		<jobstep name="Load streets" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.[lST_Street__WQ_Dataexplorers_Collision_Typed] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Load intersection pass 1" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.[lIS_Intersection__WQ_Dataexplorers_Collision_Typed__1] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Load ST ST IS tie" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.[lST_intersecting_IS_of_ST_crossing__WQ_Dataexplorers_Collision_Typed] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Load intersection pass 2" database_name="Development" subsystem="TSQL">
            EXEC source.[lIS_Intersection__WQ_Dataexplorers_Collision_Typed__2] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
	</job>
</workflow>
';
DECLARE @name varchar(255) = @xml.value('/workflow[1]/@name', 'varchar(255)');
DECLARE @CF_ID int;
SELECT
    @CF_ID = CF_ID
FROM
    Development.metadata.lCF_Configuration
WHERE
    CF_NAM_Configuration_Name = @name;
IF(@CF_ID is null) 
BEGIN
    INSERT INTO Development.metadata.lCF_Configuration (
        CF_TYP_CFT_ConfigurationType,
        CF_NAM_Configuration_Name,
        CF_XML_Configuration_XMLDefinition
    )
    VALUES (
        'Workflow',
        @name,
        @xml
    );
END
ELSE
BEGIN
    UPDATE Development.metadata.lCF_Configuration
    SET
        CF_XML_Configuration_XMLDefinition = @xml
    WHERE
        CF_NAM_Configuration_Name = @name;
END
