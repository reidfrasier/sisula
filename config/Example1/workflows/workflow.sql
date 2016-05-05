------------------------------- NYPD_Vehicle_Workflow -------------------------------
USE msdb;
GO
IF EXISTS (select job_id from [dbo].[sysjobs_view] where name = 'NYPD_Vehicle_Staging')
EXEC sp_delete_job
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Staging';
GO
sp_add_job
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Staging';
GO
sp_add_jobserver
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Staging';
GO
sp_add_jobstep
    @job_name = 'NYPD_Vehicle_Staging',
    @step_name = 'Log starting of job',
    @step_id = 1,
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStarting @workflowName = ''NYPD_Vehicle_Workflow'', @jobName = ''NYPD_Vehicle_Staging'', @agentJobId = $(ESCAPE_NONE(JOBID))',
    @on_success_action = 3; -- go to the next step
GO
sp_add_jobstep
    @subsystem = 'PowerShell',
    @command = '
            $files = Get-ChildItem -Path "D:\Sisula ETL\Example1\incoming" | Where-Object {$_.Name -match "[0-9]{5}_Collisions_.*\.csv"};
            If ($files.Length -eq 0) {
              Throw "No matching files were found in D:\Sisula ETL\Example1\incoming";
            }
            Else {
              ForEach ($file in $files) {
                $fullFileName = $file.FullName;
                Move-Item -Path $fullFileName -Destination "D:\Sisula ETL\Example1\work" -Force;
                Write-Output "Moved file: $fullFileName to D:\Sisula ETL\Example1\work";
              }
            };
        ',
    @proxy_name = 'RFrasier',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Staging',
    @step_name = 'Check for and move files';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.NYPD_Vehicle_CreateRawTable @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Staging',
    @step_name = 'Create raw table';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.NYPD_Vehicle_CreateInsertView @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Staging',
    @step_name = 'Create insert view';
GO
sp_add_jobstep
    @subsystem = 'PowerShell',
    @command = '
            $files = Get-ChildItem -Path "D:\Sisula ETL\Example1\work" -Recurse | Where-Object {$_.Name -match "[0-9]{5}_Collisions_.*\.csv"};
            If ($files.Length -eq 0) {
              Throw "No matching files were found in D:\Sisula ETL\Example1\work";
            }
            Else {
              ForEach ($file in $files) {
                $fullFileName = $file.FullName;
                $modifiedDate = $file.LastWriteTime;
                Invoke-Sqlcmd "EXEC source.NYPD_Vehicle_BulkInsert `"$fullFileName`", `"$modifiedDate`", @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))" -Database "Development" -ErrorAction Stop -QueryTimeout 0;
                Write-Output "Loaded file: $fullFileName";
                Move-Item -Path $fullFileName -Destination "D:\Sisula ETL\Example1\archive" -Force;
                Write-Output "Moved file: $fullFileName to D:\Sisula ETL\Example1\archive";
              }
            };
        ',
    @database_name = 'Development',
    @proxy_name = 'RFrasier',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Staging',
    @step_name = 'Bulk insert';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.NYPD_Vehicle_CreateSplitViews @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Staging',
    @step_name = 'Create split views';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.NYPD_Vehicle_CreateErrorViews @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Staging',
    @step_name = 'Create error views';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.NYPD_Vehicle_CreateTypedTables @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Staging',
    @step_name = 'Create typed tables';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.NYPD_Vehicle_SplitRawIntoTyped @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Staging',
    @step_name = 'Split raw into typed';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.NYPD_Vehicle_AddKeysToTyped @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Staging',
    @step_name = 'Add keys to typed';
GO
sp_add_jobstep
    @job_name = 'NYPD_Vehicle_Staging',
    @step_name = 'Log success of job',
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStopping @name = ''NYPD_Vehicle_Staging'', @status = ''Success''',
    @on_success_action = 1; -- quit with success
GO
sp_add_jobstep
    @job_name = 'NYPD_Vehicle_Staging',
    @step_name = 'Log failure of job',
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStopping @name = ''NYPD_Vehicle_Staging'', @status = ''Failure''',
    @on_success_action = 2; -- quit with failure
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Staging',
    @step_id = 2,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Staging',
    @step_id = 3,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Staging',
    @step_id = 4,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Staging',
    @step_id = 5,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Staging',
    @step_id = 6,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Staging',
    @step_id = 7,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Staging',
    @step_id = 8,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Staging',
    @step_id = 9,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Staging',
    @step_id = 10,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 12;
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Staging',
    @step_id = 10,
    -- ensure logging when last step succeeds
    @on_success_action = 4, -- go to step with id
    @on_success_step_id = 11;
GO
IF EXISTS (select job_id from [dbo].[sysjobs_view] where name = 'NYPD_Vehicle_Loading')
EXEC sp_delete_job
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Loading';
GO
sp_add_job
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Loading';
GO
sp_add_jobserver
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Loading';
GO
sp_add_jobstep
    @job_name = 'NYPD_Vehicle_Loading',
    @step_name = 'Log starting of job',
    @step_id = 1,
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStarting @workflowName = ''NYPD_Vehicle_Workflow'', @jobName = ''NYPD_Vehicle_Loading'', @agentJobId = $(ESCAPE_NONE(JOBID))',
    @on_success_action = 3; -- go to the next step
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.[lST_Street__NYPD_Vehicle_Collision_Typed] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Loading',
    @step_name = 'Load streets';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.[lIS_Intersection__NYPD_Vehicle_Collision_Typed__1] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Loading',
    @step_name = 'Load intersection pass 1';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.[lST_intersecting_IS_of_ST_crossing__NYPD_Vehicle_Collision_Typed] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    @on_success_action = 3,
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Loading',
    @step_name = 'Load ST ST IS tie';
GO
sp_add_jobstep
    @subsystem = 'TSQL',
    @command = '
            EXEC source.[lIS_Intersection__NYPD_Vehicle_Collision_Typed__2] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        ',
    @database_name = 'Development',
    -- mandatory parameters below and optional ones above this line
    @job_name = 'NYPD_Vehicle_Loading',
    @step_name = 'Load intersection pass 2';
GO
sp_add_jobstep
    @job_name = 'NYPD_Vehicle_Loading',
    @step_name = 'Log success of job',
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStopping @name = ''NYPD_Vehicle_Loading'', @status = ''Success''',
    @on_success_action = 1; -- quit with success
GO
sp_add_jobstep
    @job_name = 'NYPD_Vehicle_Loading',
    @step_name = 'Log failure of job',
    @subsystem = 'TSQL',
    @database_name = 'Development',
    @command = 'EXEC metadata._JobStopping @name = ''NYPD_Vehicle_Loading'', @status = ''Failure''',
    @on_success_action = 2; -- quit with failure
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Loading',
    @step_id = 2,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 7;
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Loading',
    @step_id = 3,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 7;
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Loading',
    @step_id = 4,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 7;
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Loading',
    @step_id = 5,
    -- ensure logging when any step fails
    @on_fail_action = 4, -- go to step with id
    @on_fail_step_id = 7;
GO
sp_update_jobstep
    @job_name = 'NYPD_Vehicle_Loading',
    @step_id = 5,
    -- ensure logging when last step succeeds
    @on_success_action = 4, -- go to step with id
    @on_success_step_id = 6;
GO
-- The workflow definition used when generating the above
DECLARE @xml XML = N'<workflow name="NYPD_Vehicle_Workflow">
	<variable name="incomingPath" value="D:\Sisula ETL\Example1\incoming"/>
	<variable name="workPath" value="D:\Sisula ETL\Example1\work"/>
	<variable name="archivePath" value="D:\Sisula ETL\Example1\archive"/>
	<variable name="filenamePattern" value="[0-9]{5}_Collisions_.*\.csv"/>
	<variable name="quitWithSuccess" value="1"/>
	<variable name="quitWithFailure" value="2"/>
	<variable name="goToTheNextStep" value="3"/>
	<variable name="goToStepWithId" value="4"/>
	<variable name="queryTimeout" value="0"/>
	<variable name="powerShellProxyName" value="RFrasier"/>
	<variable name="extraOptions" value="-Recurse"/>
	<variable name="parameters" value="@agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))"/>
	<job name="NYPD_Vehicle_Staging">
		<jobstep name="Check for and move files" subsystem="PowerShell" proxy_name="RFrasier" on_success_action="3">
            $files = Get-ChildItem -Path "%DataPath%\%FolderName%\incoming" | Where-Object {$_.Name -match "[0-9]{5}_Collisions_.*\.csv"};
            If ($files.Length -eq 0) {
              Throw "No matching files were found in %DataPath%\%FolderName%\incoming";
            }
            Else {
              ForEach ($file in $files) {
                $fullFileName = $file.FullName;
                Move-Item -Path $fullFileName -Destination "%DataPath%\%FolderName%\work" -Force;
                Write-Output "Moved file: $fullFileName to %DataPath%\%FolderName%\work";
              }
            };
        </jobstep>
		<jobstep name="Create raw table" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.NYPD_Vehicle_CreateRawTable @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Create insert view" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.NYPD_Vehicle_CreateInsertView @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Bulk insert" database_name="Development" subsystem="PowerShell" proxy_name="RFrasier" on_success_action="3">
            $files = Get-ChildItem -Path "%DataPath%\%FolderName%\work" -Recurse | Where-Object {$_.Name -match "[0-9]{5}_Collisions_.*\.csv"};
            If ($files.Length -eq 0) {
              Throw "No matching files were found in %DataPath%\%FolderName%\work";
            }
            Else {
              ForEach ($file in $files) {
                $fullFileName = $file.FullName;
                $modifiedDate = $file.LastWriteTime;
                Invoke-Sqlcmd "EXEC source.NYPD_Vehicle_BulkInsert `"$fullFileName`", `"$modifiedDate`", @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))" -Database "Development" -ErrorAction Stop -QueryTimeout 0;
                Write-Output "Loaded file: $fullFileName";
                Move-Item -Path $fullFileName -Destination "%DataPath%\%FolderName%\archive" -Force;
                Write-Output "Moved file: $fullFileName to %DataPath%\%FolderName%\archive";
              }
            };
        </jobstep>
		<jobstep name="Create split views" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.NYPD_Vehicle_CreateSplitViews @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Create error views" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.NYPD_Vehicle_CreateErrorViews @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Create typed tables" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.NYPD_Vehicle_CreateTypedTables @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Split raw into typed" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.NYPD_Vehicle_SplitRawIntoTyped @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Add keys to typed" database_name="Development" subsystem="TSQL">
            EXEC source.NYPD_Vehicle_AddKeysToTyped @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
	</job>
	<job name="NYPD_Vehicle_Loading">
		<jobstep name="Load streets" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.[lST_Street__NYPD_Vehicle_Collision_Typed] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Load intersection pass 1" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.[lIS_Intersection__NYPD_Vehicle_Collision_Typed__1] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Load ST ST IS tie" database_name="Development" subsystem="TSQL" on_success_action="3">
            EXEC source.[lST_intersecting_IS_of_ST_crossing__NYPD_Vehicle_Collision_Typed] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Load intersection pass 2" database_name="Development" subsystem="TSQL">
            EXEC source.[lIS_Intersection__NYPD_Vehicle_Collision_Typed__2] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
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
