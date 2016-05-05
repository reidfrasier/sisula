------------------------------- %System%_%Source%_Workflow -------------------------------
USE msdb;
GO
-- The workflow definition used when generating the above
DECLARE @xml XML = N'<workflow name="%System%_%Source%_Workflow">
	<variable name="stage" value="%SourceDatabase%"/>
	<variable name="incomingPath" value="%SisulaPath%Example\data\incoming"/>
	<variable name="workPath" value="%SisulaPath%Example\data\work"/>
	<variable name="archivePath" value="%SisulaPath%Example\data\archive"/>
	<variable name="filenamePattern" value="[0-9]{5}_Collisions_.*\.csv"/>
	<variable name="quitWithSuccess" value="1"/>
	<variable name="quitWithFailure" value="2"/>
	<variable name="goToTheNextStep" value="3"/>
	<variable name="goToStepWithId" value="4"/>
	<variable name="queryTimeout" value="0"/>
	<variable name="extraOptions" value="-Recurse"/>
	<variable name="parameters" value="@agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))"/>
	<job name="%System%_%Source%_Staging">
		<variable name="tableName" value="MyTable"/>
		<jobstep name="Check for and move files" subsystem="PowerShell" on_success_action="3">
            $files = @(Get-ChildItem FileSystem::"%SisulaPath%Example\data\incoming" | Where-Object {$_.Name -match "[0-9]{5}_Collisions_.*\.csv"})
            If ($files.length -eq 0) {
              Throw "No matching files were found in %SisulaPath%Example\data\incoming"
            } Else {
                ForEach ($file in $files) {
                    $fullFilename = $file.FullName
                    Move-Item $fullFilename %SisulaPath%Example\data\work -force
                    Write-Output "Moved file: $fullFilename to %SisulaPath%Example\data\work"
                }
            }
        </jobstep>
		<jobstep name="Create raw table" database_name="%SourceDatabase%" subsystem="TSQL" on_success_action="3">
            EXEC %SourceSchema%.%System%_%Source%_CreateRawTable @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Create insert view" database_name="%SourceDatabase%" subsystem="TSQL" on_success_action="3">
            EXEC %SourceSchema%.%System%_%Source%_CreateInsertView @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Bulk insert" database_name="%SourceDatabase%" subsystem="PowerShell" on_success_action="3">
            $files = @(Get-ChildItem -Recurse FileSystem::"%SisulaPath%Example\data\work" | Where-Object {$_.Name -match "[0-9]{5}_Collisions_.*\.csv"})
            If ($files.length -eq 0) {
              Throw "No matching files were found in %SisulaPath%Example\data\work"
            } Else {
                ForEach ($file in $files) {
                    $fullFilename = $file.FullName
                    $modifiedDate = $file.LastWriteTime
                    Invoke-Sqlcmd "EXEC %SourceSchema%.%System%_%Source%_BulkInsert ''$fullFilename'', ''$modifiedDate'', @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))" -Database "%SourceDatabase%" -ErrorAction Stop -QueryTimeout 0
                    Write-Output "Loaded file: $fullFilename"
                    Move-Item $fullFilename %SisulaPath%Example\data\archive -force
                    Write-Output "Moved file: $fullFilename to %SisulaPath%Example\data\archive"
                }
            }
        </jobstep>
		<jobstep name="Create split views" database_name="%SourceDatabase%" subsystem="TSQL" on_success_action="3">
            EXEC %SourceSchema%.%System%_%Source%_CreateSplitViews @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Create error views" database_name="%SourceDatabase%" subsystem="TSQL" on_success_action="3">
            EXEC %SourceSchema%.%System%_%Source%_CreateErrorViews @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Create typed tables" database_name="%SourceDatabase%" subsystem="TSQL" on_success_action="3">
            EXEC %SourceSchema%.%System%_%Source%_CreateTypedTables @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Split raw into typed" database_name="%SourceDatabase%" subsystem="TSQL" on_success_action="3">
            EXEC %SourceSchema%.%System%_%Source%_SplitRawIntoTyped @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Add keys to typed" database_name="%SourceDatabase%" subsystem="TSQL">
            EXEC %SourceSchema%.%System%_%Source%_AddKeysToTyped @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
	</job>
	<job name="%System%_%Source%_Loading">
		<jobstep name="Load streets" database_name="%SourceDatabase%" subsystem="TSQL" on_success_action="3">
            EXEC %SourceSchema%.[lST_Street__%System%_%Source%_Collision_Typed] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Load intersection pass 1" database_name="%SourceDatabase%" subsystem="TSQL" on_success_action="3">
            EXEC %SourceSchema%.[lIS_Intersection__%System%_%Source%_Collision_Typed__1] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Load ST ST IS tie" database_name="%SourceDatabase%" subsystem="TSQL" on_success_action="3">
            EXEC %SourceSchema%.[lST_intersecting_IS_of_ST_crossing__%System%_%Source%_Collision_Typed] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
		<jobstep name="Load intersection pass 2" database_name="%SourceDatabase%" subsystem="TSQL">
            EXEC %SourceSchema%.[lIS_Intersection__%System%_%Source%_Collision_Typed__2] @agentJobId = $(ESCAPE_NONE(JOBID)), @agentStepId = $(ESCAPE_NONE(STEPID))
        </jobstep>
	</job>
</workflow>
';
DECLARE @name varchar(255) = @xml.value('/workflow[1]/@name', 'varchar(255)');
DECLARE @CF_ID int;
SELECT
    @CF_ID = CF_ID
FROM
    Stage.metadata.lCF_Configuration
WHERE
    CF_NAM_Configuration_Name = @name;
IF(@CF_ID is null) 
BEGIN
    INSERT INTO Stage.metadata.lCF_Configuration (
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
    UPDATE Stage.metadata.lCF_Configuration
    SET
        CF_XML_Configuration_XMLDefinition = @xml
    WHERE
        CF_NAM_Configuration_Name = @name;
END
