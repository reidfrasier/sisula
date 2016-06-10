--------------------------- Send Email Report ---------------------------
IF Object_Id('metadata._SendEmailReport', 'P') IS NOT NULL
  DROP PROCEDURE metadata._WorkStarting;
GO

CREATE PROCEDURE metadata._SendEmailReport (
  @agentStepId smallint = NULL ,
  @agentJobId uniqueidentifier = NULL
)
AS
  BEGIN
    --declare variables
    DECLARE @tableHTML nvarchar(max)
    --DECLARE @now datetimeoffset(7) = SYSDATETIMEOFFSET()
    --DECLARE @startOfDay datetime2(7) = DATETIMEFROMPARTS(DATEPART(year, @now), DATEPART(month, @now), 8, 0, 0 ,0, 0)
    SET @tableHTML =
    N'<H1>SQL Server Agent Job Status Report</H1>' +
    N'<table border="1">' +
    N'<tr><th>JobId</th>' +
    N'<th>JobStartDateTime</th>' +
    N'<th>JobEndDateTime</th>' +
    N'<th>JobName</th>' +
    N'<th>JobExecutionStatus</th>' +
    N'<th>WorkName</th>' +
    N'<th>WorkInvocationUser</th>' +
    N'<th>WorkExecutionStatus</th>' +
    N'<th>WorkErrorLine</th>' +
    N'<th>WorkErrorMessage</th>' +
    N'<th>SourceContainerName</th>' +
    N'<th>SourceContainerType</th>' +
    N'<th>TargetContainerType</th>' +
    N'<th>OperationRowCount</th>' +
    N'<th>OperationTypeName</th></tr>' +
    CAST((
           SELECT
               td = ISNULL(JobId, ''), '',
               td = ISNULL(JobStartDateTime, ''), '',
               td = ISNULL(JobEndDateTime, ''), '',
               td = ISNULL(JobName, ''), '',
               td = ISNULL(JobExecutionStatus, ''), '',
               td = ISNULL(WorkName, ''), '',
               td = ISNULL(WorkInvocationUser, ''), '',
               td = ISNULL(WorkExecutionStatus, ''), '',
               td = ISNULL(WorkErrorLine, ''), '',
               td = ISNULL(WorkErrorMessage, ''), '',
               td = ISNULL(SourceContainerName, ''), '',
               td = ISNULL(SourceContainerType, ''), '',
               td = ISNULL(TargetContainerType, ''), '',
               td = ISNULL(OperationRowCount, ''), '',
               td = ISNULL(OperationTypeName, '')
           FROM (
                  SELECT
                    JB_ID AS JobId,
                    JB_STA_Job_Start AS JobStartDateTime,
                    JB_END_Job_End AS JobEndDateTime,
                    JB_NAM_Job_Name AS JobName,
                    JB_EST_ChangedAt AS JobExecutionStatusChangedAt,
                    JB_EST_EST_ExecutionStatus AS JobExecutionStatus,
                    JB_AID_Job_AgentJobId AS JobAgentJobId,
                    WO_ID AS WorkId,
                    WO_STA_Work_Start AS WorkStartDateTime,
                    WO_END_Work_End AS WorkEndDateTime,
                    WO_NAM_Work_Name AS WorkName,
                    WO_USR_Work_InvocationUser AS WorkInvocationUser,
                    WO_ROL_Work_InvocationRole AS WorkInvocationRole,
                    WO_EST_ChangedAt AS WorkExecutionStatusChangedAt,
                    WO_EST_EST_ExecutionStatus AS WorkExecutionStatus,
                    WO_ERL_Work_ErrorLine AS WorkErrorLine,
                    WO_ERM_Work_ErrorMessage AS WorkErrorMessage,
                    WO_AID_Work_AgentStepId AS WorkAgentStepId,
                    SourceContainerId,
                    SourceContainerName,
                    SourceContainerType,
                    SourceContainerDiscoveredChangedAt,
                    SourceContainerDiscoveredDateTime,
                    SourceContainerCreatedDateTime,
                    TargetContainerId,
                    TargetContainerType,
                    TargetContainerDiscoveredChangedAt,
                    TargetContainerDiscoveredDateTime,
                    TargetContainerCreatedDateTime,
                    OperationId,
                    OperationChangedAt,
                    OperationRowCount,
                    OperationTypeName
                  FROM
                    [metadata].[lJB_Job] AS [job]
                    FULL JOIN
                    [metadata].[lWO_part_JB_of] AS [tie1]
                      ON
                        [job].[JB_ID] = [tie1].[JB_ID_of]
                    FULL JOIN
                    [metadata].[lWO_Work] AS [work]
                      ON
                        [tie1].[WO_ID_part] = [work].[WO_ID]
                    LEFT JOIN -- ensure we only look at rows for which a WO_ID exists
                    [metadata].[lWO_operates_CO_source_CO_target_OP_with] AS [tie2]
                      ON
                        [work].[WO_ID] = [tie2].[WO_ID_operates]
                    LEFT JOIN
                    (
                      SELECT
                        CO_ID AS SourceContainerId,
                        CO_NAM_Container_Name AS SourceContainerName,
                        CO_TYP_COT_ContainerType AS SourceContainerType,
                        CO_DSC_ChangedAt AS SourceContainerDiscoveredChangedAt,
                        CO_DSC_Container_Discovered AS SourceContainerDiscoveredDateTime,
                        CO_CRE_Container_Created AS SourceContainerCreatedDateTime
                      FROM
                        [metadata].[lCO_Container]
                    ) sourceContainer
                      ON
                        [tie2].[CO_ID_source] = [sourceContainer].[SourceContainerId]
                    LEFT JOIN
                    (
                      SELECT
                        CO_ID AS TargetContainerId,
                        CO_NAM_Container_Name AS TargetContainerName,
                        CO_TYP_COT_ContainerType AS TargetContainerType,
                        CO_DSC_ChangedAt AS TargetContainerDiscoveredChangedAt,
                        CO_DSC_Container_Discovered AS TargetContainerDiscoveredDateTime,
                        CO_CRE_Container_Created AS TargetContainerCreatedDateTime
                      FROM
                        [metadata].[lCO_Container]
                    ) targetContainer
                      ON
                        [tie2].[CO_ID_target] = [targetContainer].[TargetContainerId]
                    FULL JOIN
                    (
                      SELECT
                        [allOperations].[OP_ID] AS [OperationId],
                        [tableOperations].[OperationChangedAt],
                        [tableOperations].[OperationRowCount],
                        [tableOperations].[OperationTypeName]
                      FROM
                        [metadata].[OP_Operations] AS allOperations
                        LEFT JOIN
                        (
                          SELECT
                            OP_INS_OP_ID AS OperationId,
                            OP_INS_ChangedAt AS OperationChangedAt,
                            OP_INS_Operations_Inserts AS OperationRowCount,
                            'Insert' AS OperationTypeName
                          FROM
                            [metadata].[OP_INS_Operations_Inserts]
                          UNION ALL
                          SELECT
                            OP_UPD_OP_ID AS OperationId,
                            OP_UPD_ChangedAt AS OperationChangedAt,
                            OP_UPD_Operations_Updates AS OperationRowCount,
                            'Update' AS OperationTypeName
                          FROM
                            [metadata].[OP_UPD_Operations_Updates]
                          UNION ALL
                          SELECT
                            OP_DEL_OP_ID AS OperationId,
                            OP_DEL_ChangedAt AS OperationChangedAt,
                            OP_DEL_Operations_Deletes AS OperationRowCount,
                            'Delete' AS OperationTypeName
                          FROM
                            [metadata].[OP_DEL_Operations_Deletes]
                        ) tableOperations
                          ON
                            [allOperations].[OP_ID] = [tableOperations].[OperationId]
                    ) unionOperations
                      ON
                        [tie2].[OP_ID_with] = [unionOperations].[OperationId]
                ) AS jobdata
           WHERE
             --JobStartDateTime >= @startOfDay
             JobAgentJobId = @agentJobId
           ORDER BY JobId ASC
           FOR XML PATH('tr'), TYPE
         ) AS nvarchar(max)) + N'</table>';
    --send email
    EXEC msdb.dbo.sp_send_dbmail
      @profile_name = 'APP_DBMAIL_PROFILE',
      @recipients = 'reid.frasier@mlp.com',
      @subject = 'SQL Server Agent Job Status Report',
      @body = @tableHTML,
      @body_format = 'HTML';
  END
GO