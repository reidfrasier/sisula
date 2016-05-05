-- create schema if it does not exist
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dbo')
BEGIN
    EXEC( 'CREATE SCHEMA dbo' );
END
GO
-- creates utility functions needed in the staging
IF Object_Id('dbo.Splitter', 'FT') IS NOT NULL
DROP FUNCTION [dbo].[Splitter];
GO
IF Object_Id('dbo.IsType', 'FS') IS NOT NULL
DROP FUNCTION [dbo].[IsType];
GO
IF Object_Id('dbo.ColumnSplitter', 'PC') IS NOT NULL
DROP PROCEDURE [dbo].[ColumnSplitter];
GO
IF EXISTS (
	SELECT
		*
	FROM
		sys.assemblies
	WHERE
		name = 'dboUtilities'
)
DROP ASSEMBLY dboUtilities;
IF NOT EXISTS (
	SELECT
		*
	FROM
		sys.assemblies
	WHERE
		name = 'dboUtilities'
)
BEGIN TRY -- using Microsoft.SQLServer.Types version 13 (2016)
	CREATE ASSEMBLY dboUtilities
	AUTHORIZATION dbo
	FROM 'undefinedcode\Utilities2016.dll'
	WITH PERMISSION_SET = SAFE;
	PRINT 'The .NET CLR for SQL Server 2016 was installed.'
END TRY
BEGIN CATCH
	PRINT 'The .NET CLR for SQL Server 2016 was NOT installed.'
END CATCH
IF NOT EXISTS (
	SELECT
		*
	FROM
		sys.assemblies
	WHERE
		name = 'dboUtilities'
)
BEGIN TRY -- using Microsoft.SQLServer.Types version 12 (2014)
	CREATE ASSEMBLY dboUtilities
	AUTHORIZATION dbo
	FROM 'undefinedcode\Utilities2014.dll'
	WITH PERMISSION_SET = SAFE;
	PRINT 'The .NET CLR for SQL Server 2014 was installed.'
END TRY
BEGIN CATCH
	PRINT 'The .NET CLR for SQL Server 2014 was NOT installed.'
END CATCH
IF NOT EXISTS (
	SELECT
		*
	FROM
		sys.assemblies
	WHERE
		name = 'dboUtilities'
)
BEGIN TRY -- using Microsoft.SQLServer.Types version 11 (2012)
	CREATE ASSEMBLY dboUtilities
	AUTHORIZATION dbo
	FROM 'undefinedcode\Utilities2012.dll'
	WITH PERMISSION_SET = SAFE;
	PRINT 'The .NET CLR for SQL Server 2012 was installed.'
END TRY
BEGIN CATCH
	PRINT 'The .NET CLR for SQL Server 2012 was NOT installed.'
END CATCH
IF NOT EXISTS (
	SELECT
		*
	FROM
		sys.assemblies
	WHERE
		name = 'dboUtilities'
)
BEGIN TRY -- using Microsoft.SQLServer.Types version 10 (2008)
	CREATE ASSEMBLY dboUtilities
	AUTHORIZATION dbo
	FROM 'undefinedcode\Utilities2008.dll'
	WITH PERMISSION_SET = SAFE;
	PRINT 'The .NET CLR for SQL Server 2008 was installed.'
END TRY
BEGIN CATCH
	PRINT 'The .NET CLR for SQL Server 2008 was NOT installed.'
END CATCH
GO
CREATE FUNCTION [dbo].Splitter(@row AS nvarchar(max), @pattern AS nvarchar(4000))
RETURNS TABLE (
	[match] nvarchar(max),
	[index] int
) AS EXTERNAL NAME dboUtilities.Splitter.InitMethod;
GO
CREATE FUNCTION [dbo].IsType(@dataValue AS nvarchar(max), @dataType AS nvarchar(4000))
RETURNS BIT
AS EXTERNAL NAME dboUtilities.IsType.InitMethod;
GO
CREATE PROCEDURE [dbo].ColumnSplitter(
	@table AS nvarchar(4000),
	@column AS nvarchar(4000),
	@pattern AS nvarchar(4000),
	@includeColumns AS nvarchar(4000) = null
)
AS EXTERNAL NAME dboUtilities.ColumnSplitter.InitMethod;
GO
IF NOT EXISTS (
    SELECT value
    FROM sys.configurations
    WHERE name = 'clr enabled'
      AND value = 1
)
BEGIN
    EXEC sp_configure 'clr enabled', 1;
    reconfigure with override;
END
GO
IF Object_ID('dbo.%Source%_CreateRawTable', 'P') IS NOT NULL
DROP PROCEDURE [dbo].[%Source%_CreateRawTable];
GO
--------------------------------------------------------------------------
-- Procedure: %Source%_CreateRawTable
--
-- This table holds the 'raw' loaded data.
--
-- row
-- Holds a row loaded from a file.
--
-- _id
-- This sequence is generated in order to keep a lineage through the
-- staging process. If a single file has been loaded, this corresponds
-- to the row number in the file.
--
-- metadata_CO_ID
-- A number containing the file id, which either points to metadata
-- if its used or is otherwise an incremented number per file.
--
-- _timestamp
-- The time the row was created.
--
-- Generated: Fri Mar 11 13:01:07 UTC+0800 2016 by rfrasier
-- From: HKWTR130 in the AD domain
--------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[%Source%_CreateRawTable] (
    @agentJobId uniqueidentifier = null,
    @agentStepId smallint = null
)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @workId int;
DECLARE @operationsId int;
DECLARE @theErrorLine int;
DECLARE @theErrorMessage varchar(555);
DECLARE @theErrorSeverity int;
DECLARE @theErrorState int;
EXEC Stage.metadata._WorkStarting
    @configurationName = '%Source%', 
    @configurationType = 'Source', 
    @WO_ID = @workId OUTPUT, 
    @name = '%Source%_CreateRawTable',
    @agentStepId = @agentStepId,
    @agentJobId = @agentJobId
BEGIN TRY
    IF Object_ID('dbo.%Source%_Raw', 'U') IS NOT NULL
    DROP TABLE [dbo].[%Source%_Raw];
    CREATE TABLE [dbo].[%Source%_Raw] (
        _id int identity(1,1) not null,
        _file AS metadata_CO_ID, -- keep an alias for backwards compatibility
        metadata_CO_ID int not null default 0,
        metadata_JB_ID int not null default 0,
        _timestamp datetime not null default getdate(),
        [row] varchar(1000), 
        constraint [pkdbo_%Source%_Raw] primary key(
            _id asc
        )
    );
    EXEC Stage.metadata._WorkStopping @workId, 'Success';
END TRY
BEGIN CATCH
	SELECT
		@theErrorLine = ERROR_LINE(),
		@theErrorMessage = ERROR_MESSAGE(),
        @theErrorSeverity = ERROR_SEVERITY(),
        @theErrorState = ERROR_STATE();
    EXEC Stage.metadata._WorkStopping
        @WO_ID = @workId, 
        @status = 'Failure', 
        @errorLine = @theErrorLine, 
        @errorMessage = @theErrorMessage;
    -- Propagate the error
    RAISERROR(
        @theErrorMessage,
        @theErrorSeverity,
        @theErrorState
    ); 
END CATCH
END
GO
IF Object_ID('dbo.%Source%_CreateInsertView', 'P') IS NOT NULL
DROP PROCEDURE [dbo].[%Source%_CreateInsertView];
GO
--------------------------------------------------------------------------
-- Procedure: %Source%_CreateInsertView
--
-- This view is created as exposing the single column that will be
-- the target of the BULK INSERT operation, since it cannot insert
-- into a table with multiple columns without a format file.
--
-- Generated: Fri Mar 11 13:01:07 UTC+0800 2016 by rfrasier
-- From: HKWTR130 in the AD domain
--------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[%Source%_CreateInsertView] (
    @agentJobId uniqueidentifier = null,
    @agentStepId smallint = null
)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @workId int;
DECLARE @operationsId int;
DECLARE @theErrorLine int;
DECLARE @theErrorMessage varchar(555);
DECLARE @theErrorSeverity int;
DECLARE @theErrorState int;
EXEC Stage.metadata._WorkStarting
    @configurationName = '%Source%', 
    @configurationType = 'Source', 
    @WO_ID = @workId OUTPUT, 
    @name = '%Source%_CreateInsertView',
    @agentStepId = @agentStepId,
    @agentJobId = @agentJobId
BEGIN TRY
    IF Object_ID('dbo.%Source%_Insert', 'V') IS NOT NULL
    DROP VIEW [dbo].[%Source%_Insert];
    EXEC('
    CREATE VIEW [dbo].[%Source%_Insert]
    AS
    SELECT
        [row]
    FROM
        [dbo].[%Source%_Raw];
    ');
    EXEC Stage.metadata._WorkStopping @workId, 'Success';
END TRY
BEGIN CATCH
	SELECT
		@theErrorLine = ERROR_LINE(),
		@theErrorMessage = ERROR_MESSAGE(),
        @theErrorSeverity = ERROR_SEVERITY(),
        @theErrorState = ERROR_STATE();
    EXEC Stage.metadata._WorkStopping
        @WO_ID = @workId, 
        @status = 'Failure', 
        @errorLine = @theErrorLine, 
        @errorMessage = @theErrorMessage;
    -- Propagate the error
    RAISERROR(
        @theErrorMessage,
        @theErrorSeverity,
        @theErrorState
    ); 
END CATCH
END
GO
IF Object_ID('dbo.%Source%_BulkInsert', 'P') IS NOT NULL
DROP PROCEDURE [dbo].[%Source%_BulkInsert];
GO
--------------------------------------------------------------------------
-- Procedure: %Source%_BulkInsert
--
-- This procedure performs a BULK INSERT of the given filename into
-- the %Source%_Insert view. The file is loaded row by row
-- into a single column holding the entire row. This ensures that no
-- data is lost when loading.
--
-- This job may called multiple times in a workflow when more than
-- one file matching a given filename pattern is found.
--
-- Generated: Fri Mar 11 13:01:07 UTC+0800 2016 by rfrasier
-- From: HKWTR130 in the AD domain
--------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[%Source%_BulkInsert] (
	@filename varchar(2000),
    @lastModified datetime,
    @agentJobId uniqueidentifier = null,
    @agentStepId smallint = null
)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @CO_ID int;
DECLARE @JB_ID int;
DECLARE @inserts int;
DECLARE @updates int;
DECLARE @workId int;
DECLARE @operationsId int;
DECLARE @theErrorLine int;
DECLARE @theErrorMessage varchar(555);
DECLARE @theErrorSeverity int;
DECLARE @theErrorState int;
EXEC Stage.metadata._WorkStarting
    @configurationName = '%Source%', 
    @configurationType = 'Source', 
    @WO_ID = @workId OUTPUT, 
    @name = '%Source%_BulkInsert',
    @agentStepId = @agentStepId,
    @agentJobId = @agentJobId
BEGIN TRY
EXEC Stage.metadata._WorkSourceToTarget
    @OP_ID = @operationsId OUTPUT,
    @WO_ID = @workId, 
    @sourceName = @filename, 
    @targetName = '%Source%_Insert', 
    @sourceType = 'File', 
    @targetType = 'View', 
    @sourceCreated = @lastModified, 
    @targetCreated = DEFAULT;
    IF Object_ID('dbo.%Source%_Insert', 'V') IS NOT NULL
    BEGIN
    EXEC('
        BULK INSERT [dbo].[%Source%_Insert]
        FROM ''' + @filename + '''
        WITH (
            CODEPAGE = ''ACP'',
            DATAFILETYPE = ''char'',
            FIELDTERMINATOR = ''\r\n'',
            FIRSTROW = 1,
            TABLOCK
        );
    ');
    SET @inserts = @@ROWCOUNT;
    EXEC Stage.metadata._WorkSetInserts @workId, @operationsId, @inserts;
    SET @CO_ID = ISNULL((
        SELECT TOP 1
            CO_ID
        FROM
            Stage.metadata.lCO_Container
        WHERE
            CO_NAM_Container_Name = @filename
        AND
            CO_CRE_Container_Created = @lastModified
    ), 0);
    SET @JB_ID = ISNULL((
        SELECT
            jb.JB_ID
        FROM
            Stage.metadata.lWO_part_JB_of wojb
        JOIN
            Stage.metadata.lJB_Job jb
        ON
            jb.JB_ID = wojb.JB_ID_of
        AND
            jb.JB_AID_Job_AgentJobId = @agentJobId
        WHERE
            wojb.WO_ID_part = @workId
    ), 0);
    UPDATE [dbo].[%Source%_Raw]
    SET
    metadata_CO_ID =
      case when metadata_CO_ID = 0 then @CO_ID else metadata_CO_ID end,
    metadata_JB_ID =
      case when metadata_JB_ID = 0 then @JB_ID else metadata_JB_ID end;
    SET @updates = @@ROWCOUNT;
    EXEC Stage.metadata._WorkSetUpdates @workId, @operationsId, @updates;
    END
    EXEC Stage.metadata._WorkStopping @workId, 'Success';
END TRY
BEGIN CATCH
	SELECT
		@theErrorLine = ERROR_LINE(),
		@theErrorMessage = ERROR_MESSAGE(),
        @theErrorSeverity = ERROR_SEVERITY(),
        @theErrorState = ERROR_STATE();
    EXEC Stage.metadata._WorkStopping
        @WO_ID = @workId, 
        @status = 'Failure', 
        @errorLine = @theErrorLine, 
        @errorMessage = @theErrorMessage;
    -- Propagate the error
    RAISERROR(
        @theErrorMessage,
        @theErrorSeverity,
        @theErrorState
    ); 
END CATCH
END
GO
IF Object_ID('dbo.%Source%_CreateSplitViews', 'P') IS NOT NULL
DROP PROCEDURE [dbo].[%Source%_CreateSplitViews];
GO
--------------------------------------------------------------------------
-- Procedure: %Source%_CreateSplitViews
--
-- The split view uses a CLR called the Splitter to split rows in the
-- 'raw' table into columns. The Splitter uses a regular expression in
-- which groups indicate which parts should be cut out as columns.
--
-- The view also checks data types and provide the results as well as
-- show the 'raw' cut column value, before any given transformations
-- have taken place.
--
-- If keys are defined, these keys are checked for duplicates and the
-- duplicate number can be found through the view.
--
-- Create: %Source%_Collision_Split
-- Create: %Source%_CollisionMetadata_Split
--
-- Generated: Fri Mar 11 13:01:07 UTC+0800 2016 by rfrasier
-- From: HKWTR130 in the AD domain
--------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[%Source%_CreateSplitViews] (
    @agentJobId uniqueidentifier = null,
    @agentStepId smallint = null
)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @workId int;
DECLARE @operationsId int;
DECLARE @theErrorLine int;
DECLARE @theErrorMessage varchar(555);
DECLARE @theErrorSeverity int;
DECLARE @theErrorState int;
EXEC Stage.metadata._WorkStarting
    @configurationName = '%Source%', 
    @configurationType = 'Source', 
    @WO_ID = @workId OUTPUT, 
    @name = '%Source%_CreateSplitViews',
    @agentStepId = @agentStepId,
    @agentJobId = @agentJobId
BEGIN TRY
    IF Object_ID('dbo.%Source%_Collision_Split', 'V') IS NOT NULL
    DROP VIEW [dbo].[%Source%_Collision_Split];
    EXEC('
    CREATE VIEW [dbo].[%Source%_Collision_Split]
    AS
    SELECT
        t._id,
        t.metadata_CO_ID,
        t.metadata_JB_ID,
        t._timestamp,
        m.[OccurrencePrecinctCode] as [OccurrencePrecinctCode_Match],
        t.[OccurrencePrecinctCode],
        CASE
            WHEN t.[OccurrencePrecinctCode] is not null AND [dbo].IsType(t.[OccurrencePrecinctCode], ''int'') = 0 THEN ''Conversion to int failed''
        END AS [OccurrencePrecinctCode_Error],
        m.[CollisionID] as [CollisionID_Match],
        t.[CollisionID],
        CASE
            WHEN t.[CollisionID] is not null AND [dbo].IsType(t.[CollisionID], ''int'') = 0 THEN ''Conversion to int failed''
        END AS [CollisionID_Error],
        m.[CollisionKey] as [CollisionKey_Match],
        t.[CollisionKey],
        CASE
            WHEN t.[CollisionKey] is not null AND [dbo].IsType(t.[CollisionKey], ''int'') = 0 THEN ''Conversion to int failed''
        END AS [CollisionKey_Error],
        m.[CollisionOrder] as [CollisionOrder_Match],
        t.[CollisionOrder],
        CASE
            WHEN t.[CollisionOrder] is null THEN ''Null value not allowed''
            WHEN t.[CollisionOrder] is not null AND [dbo].IsType(t.[CollisionOrder], ''tinyint'') = 0 THEN ''Conversion to tinyint failed''
        END AS [CollisionOrder_Error],
        m.[IntersectionAddress] as [IntersectionAddress_Match],
        t.[IntersectionAddress],
        CASE
            WHEN t.[IntersectionAddress] is not null AND [dbo].IsType(t.[IntersectionAddress], ''varchar(321)'') = 0 THEN ''Conversion to varchar(321) failed''
        END AS [IntersectionAddress_Error],
        m.[IntersectingStreet] as [IntersectingStreet_Match],
        t.[IntersectingStreet],
        CASE
            WHEN t.[IntersectingStreet] is null THEN ''Null value not allowed''
            WHEN t.[IntersectingStreet] is not null AND [dbo].IsType(t.[IntersectingStreet], ''varchar(321)'') = 0 THEN ''Conversion to varchar(321) failed''
        END AS [IntersectingStreet_Error],
        m.[CrossStreet] as [CrossStreet_Match],
        t.[CrossStreet],
        CASE
            WHEN t.[CrossStreet] is null THEN ''Null value not allowed''
            WHEN t.[CrossStreet] is not null AND [dbo].IsType(t.[CrossStreet], ''varchar(321)'') = 0 THEN ''Conversion to varchar(321) failed''
        END AS [CrossStreet_Error],
        m.[CollisionVehicleCount] as [CollisionVehicleCount_Match],
        t.[CollisionVehicleCount],
        CASE
            WHEN t.[CollisionVehicleCount] is not null AND [dbo].IsType(t.[CollisionVehicleCount], ''tinyint'') = 0 THEN ''Conversion to tinyint failed''
        END AS [CollisionVehicleCount_Error],
        m.[CollisionInjuredCount] as [CollisionInjuredCount_Match],
        t.[CollisionInjuredCount],
        CASE
            WHEN t.[CollisionInjuredCount] is not null AND [dbo].IsType(t.[CollisionInjuredCount], ''tinyint'') = 0 THEN ''Conversion to tinyint failed''
        END AS [CollisionInjuredCount_Error],
        m.[CollisionKilledCount] as [CollisionKilledCount_Match],
        t.[CollisionKilledCount],
        CASE
            WHEN t.[CollisionKilledCount] is not null AND [dbo].IsType(t.[CollisionKilledCount], ''tinyint'') = 0 THEN ''Conversion to tinyint failed''
        END AS [CollisionKilledCount_Error],
        ROW_NUMBER() OVER (
            PARTITION BY
                t.[IntersectingStreet],
                t.[CrossStreet],
                t.[CollisionOrder]
            ORDER BY
                t._id
        ) - 1 as measureTime_Duplicate
    FROM (
        SELECT TOP(2147483647)
            *
        FROM (
        -- this matches the data rows
        SELECT * from %SourceSchema%.%System%_%Source%_Raw WHERE [row] LIKE ''[0-9][0-9][0-9];%''
        ) src
        ORDER BY
            _id ASC
    ) forcedMaterializationTrick
    CROSS APPLY (
		SELECT
			NULLIF([1], '''') AS [OccurrencePrecinctCode],
			NULLIF([2], '''') AS [CollisionID],
			NULLIF([3], '''') AS [CollisionKey],
			NULLIF([4], '''') AS [CollisionOrder],
			NULLIF([5], '''') AS [IntersectionAddress],
			NULLIF([6], '''') AS [IntersectingStreet],
			NULLIF([7], '''') AS [CrossStreet],
			NULLIF([8], '''') AS [CollisionVehicleCount],
			NULLIF([9], '''') AS [CollisionInjuredCount],
			NULLIF([10], '''') AS [CollisionKilledCount]
		FROM (
            SELECT
                [match],
                ROW_NUMBER() OVER (ORDER BY [index] ASC) AS idx
            FROM
                [dbo].Splitter(ISNULL(forcedMaterializationTrick.[row], ''''), N''(.*?);[0-9]{4}([0-9]{9})[^;]*;(.*?);(.*?);(.*?);(.*?);(.*?);(.*?);(.*?);(.*?);'')
        ) s
        PIVOT (
            MAX([match]) FOR idx IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10])
        ) p
    ) m
    CROSS APPLY (
        SELECT
            _id,
            metadata_CO_ID,
            metadata_JB_ID,
            _timestamp,
            [OccurrencePrecinctCode] AS [OccurrencePrecinctCode],
            [CollisionID] AS [CollisionID],
            [CollisionKey] AS [CollisionKey],
            [CollisionOrder] AS [CollisionOrder],
            [IntersectionAddress] AS [IntersectionAddress],
            [IntersectingStreet] AS [IntersectingStreet],
            [CrossStreet] AS [CrossStreet],
            [CollisionVehicleCount] AS [CollisionVehicleCount],
            [CollisionInjuredCount] AS [CollisionInjuredCount],
            [CollisionKilledCount] AS [CollisionKilledCount]
    ) t;
    ');
    IF Object_ID('dbo.%Source%_CollisionMetadata_Split', 'V') IS NOT NULL
    DROP VIEW [dbo].[%Source%_CollisionMetadata_Split];
    EXEC('
    CREATE VIEW [dbo].[%Source%_CollisionMetadata_Split]
    AS
    SELECT
        t._id,
        t.metadata_CO_ID,
        t.metadata_JB_ID,
        t._timestamp,
        m.[month] as [month_Match],
        t.[month],
        CASE
            WHEN t.[month] is not null AND [dbo].IsType(t.[month], ''varchar(42)'') = 0 THEN ''Conversion to varchar(42) failed''
        END AS [month_Error],
        m.[year] as [year_Match],
        t.[year],
        CASE
            WHEN t.[year] is not null AND [dbo].IsType(t.[year], ''smallint'') = 0 THEN ''Conversion to smallint failed''
        END AS [year_Error],
        m.[notes] as [notes_Match],
        t.[notes],
        CASE
            WHEN t.[notes] is not null AND [dbo].IsType(t.[notes], ''varchar(max)'') = 0 THEN ''Conversion to varchar(max) failed''
        END AS [notes_Error]
    FROM (
        SELECT TOP(2147483647)
            *
        FROM (
        SELECT
          *
        FROM (
          SELECT
            metadata_CO_ID,
						MIN(metadata_JB_ID) as metadata_JB_ID,
            MIN(_id) as _id,
            MIN(_timestamp) as _timestamp
          FROM (
                    SELECT
                        *
                    FROM
                        %SourceSchema%.%System%_%Source%_Raw
                    WHERE
                        [row] NOT LIKE ''[0-9][0-9][0-9];%''
            ) src
          GROUP BY
            metadata_CO_ID
        ) f
        CROSS APPLY (
          SELECT
            [row] + CHAR(183) AS [text()]
          FROM (
                    SELECT
                        *
                    FROM
                        %SourceSchema%.%System%_%Source%_Raw
                    WHERE
                        [row] NOT LIKE ''[0-9][0-9][0-9];%''
            ) src
          WHERE
            src.metadata_CO_ID = f.metadata_CO_ID
          FOR XML PATH('''')
        ) c ([row])
        ) src
        ORDER BY
            _id ASC
    ) forcedMaterializationTrick
    CROSS APPLY (
		SELECT
			NULLIF([1], '''') AS [month],
			NULLIF([2], '''') AS [year],
			NULLIF([3], '''') AS [notes]
		FROM (
            SELECT
                [match],
                ROW_NUMBER() OVER (ORDER BY [index] ASC) AS idx
            FROM
                [dbo].Splitter(ISNULL(forcedMaterializationTrick.[row], ''''), N''(?=.*?(\w+)\s+[0-9]{4})?(?=.*?\w+\s+([0-9]{4}))?(?=.*?NOTES[^:]*:(.*))?'')
        ) s
        PIVOT (
            MAX([match]) FOR idx IN ([1], [2], [3])
        ) p
    ) m
    CROSS APPLY (
        SELECT
            _id,
            metadata_CO_ID,
            metadata_JB_ID,
            _timestamp,
            [month] AS [month],
            [year] AS [year],
            LTRIM(REPLACE([notes], ''�'', '' '')) AS [notes]
    ) t;
    ');
    EXEC Stage.metadata._WorkStopping @workId, 'Success';
END TRY
BEGIN CATCH
	SELECT
		@theErrorLine = ERROR_LINE(),
		@theErrorMessage = ERROR_MESSAGE(),
        @theErrorSeverity = ERROR_SEVERITY(),
        @theErrorState = ERROR_STATE();
    EXEC Stage.metadata._WorkStopping
        @WO_ID = @workId, 
        @status = 'Failure', 
        @errorLine = @theErrorLine, 
        @errorMessage = @theErrorMessage;
    -- Propagate the error
    RAISERROR(
        @theErrorMessage,
        @theErrorSeverity,
        @theErrorState
    ); 
END CATCH
END
GO
IF Object_ID('dbo.%Source%_CreateErrorViews', 'P') IS NOT NULL
DROP PROCEDURE [dbo].[%Source%_CreateErrorViews];
GO
--------------------------------------------------------------------------
-- Procedure: %Source%_CreateErrorViews
--
-- The created error views can be used to find rows that have errors of
-- the following kinds: 
-- 
-- Type casting errors
-- These errors occur when a value cannot be cast to its designated 
-- datatype, for example when trying to cast 'A' to an int.
--
-- Key duplicate errors
-- These errors occur when a primary key is defined and duplicates of
-- that key is found in the tables.
--
-- Create: %Source%_Collision_Error
-- Create: %Source%_CollisionMetadata_Error
--
-- Generated: Fri Mar 11 13:01:07 UTC+0800 2016 by rfrasier
-- From: HKWTR130 in the AD domain
--------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[%Source%_CreateErrorViews] (
    @agentJobId uniqueidentifier = null,
    @agentStepId smallint = null
)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @workId int;
DECLARE @operationsId int;
DECLARE @theErrorLine int;
DECLARE @theErrorMessage varchar(555);
DECLARE @theErrorSeverity int;
DECLARE @theErrorState int;
EXEC Stage.metadata._WorkStarting
    @configurationName = '%Source%', 
    @configurationType = 'Source', 
    @WO_ID = @workId OUTPUT, 
    @name = '%Source%_CreateInsertView',
    @agentStepId = @agentStepId,
    @agentJobId = @agentJobId
BEGIN TRY
    IF Object_ID('dbo.%Source%_Collision_Error', 'V') IS NOT NULL
    DROP VIEW [dbo].[%Source%_Collision_Error];
    EXEC('
    CREATE VIEW [dbo].[%Source%_Collision_Error] 
    AS
    SELECT
        *
    FROM
        [dbo].[%Source%_Collision_Split]
    WHERE
        measureTime_Duplicate > 0
    OR
        [OccurrencePrecinctCode_Error] is not null
    OR
        [CollisionID_Error] is not null
    OR
        [CollisionKey_Error] is not null
    OR
        [CollisionOrder_Error] is not null
    OR
        [IntersectionAddress_Error] is not null
    OR
        [IntersectingStreet_Error] is not null
    OR
        [CrossStreet_Error] is not null
    OR
        [CollisionVehicleCount_Error] is not null
    OR
        [CollisionInjuredCount_Error] is not null
    OR
        [CollisionKilledCount_Error] is not null;
    ');
    IF Object_ID('dbo.%Source%_CollisionMetadata_Error', 'V') IS NOT NULL
    DROP VIEW [dbo].[%Source%_CollisionMetadata_Error];
    EXEC('
    CREATE VIEW [dbo].[%Source%_CollisionMetadata_Error] 
    AS
    SELECT
        *
    FROM
        [dbo].[%Source%_CollisionMetadata_Split]
    WHERE
        [month_Error] is not null
    OR
        [year_Error] is not null
    OR
        [notes_Error] is not null;
    ');
    EXEC Stage.metadata._WorkStopping @workId, 'Success';
END TRY
BEGIN CATCH
	SELECT
		@theErrorLine = ERROR_LINE(),
		@theErrorMessage = ERROR_MESSAGE(),
        @theErrorSeverity = ERROR_SEVERITY(),
        @theErrorState = ERROR_STATE();
    EXEC Stage.metadata._WorkStopping
        @WO_ID = @workId, 
        @status = 'Failure', 
        @errorLine = @theErrorLine, 
        @errorMessage = @theErrorMessage;
    -- Propagate the error
    RAISERROR(
        @theErrorMessage,
        @theErrorSeverity,
        @theErrorState
    ); 
END CATCH
END
GO
IF Object_ID('dbo.%Source%_CreateTypedTables', 'P') IS NOT NULL
DROP PROCEDURE [dbo].[%Source%_CreateTypedTables];
GO
--------------------------------------------------------------------------
-- Procedure: %Source%_CreateTypedTables
--
-- The typed tables hold the data that make it through the process
-- without errors. Columns here have the data types defined in the
-- source XML definition.
--
-- Metadata columns, such as _id, can be used to backtrack from
-- a value to the actual row from where it came.
--
-- Create: %Source%_Collision_Typed
-- Create: %Source%_CollisionMetadata_Typed
--
-- Generated: Fri Mar 11 13:01:07 UTC+0800 2016 by rfrasier
-- From: HKWTR130 in the AD domain
--------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[%Source%_CreateTypedTables] (
    @agentJobId uniqueidentifier = null,
    @agentStepId smallint = null
)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @workId int;
DECLARE @operationsId int;
DECLARE @theErrorLine int;
DECLARE @theErrorMessage varchar(555);
DECLARE @theErrorSeverity int;
DECLARE @theErrorState int;
EXEC Stage.metadata._WorkStarting
    @configurationName = '%Source%', 
    @configurationType = 'Source', 
    @WO_ID = @workId OUTPUT, 
    @name = '%Source%_CreateTypedTables',
    @agentStepId = @agentStepId,
    @agentJobId = @agentJobId
BEGIN TRY
    IF Object_ID('dbo.%Source%_Collision_Typed', 'U') IS NOT NULL
    DROP TABLE [dbo].[%Source%_Collision_Typed];
    CREATE TABLE [dbo].[%Source%_Collision_Typed] (
        _id int not null,
        _file AS metadata_CO_ID, -- keep an alias for backwards compatibility
        metadata_CO_ID int not null,
        metadata_JB_ID int not null,
        _timestamp datetime not null default getdate(),
        _measureTime as cast(HashBytes('MD5', CONVERT(varchar(max), [IntersectingStreet], 126) + CHAR(183) + CONVERT(varchar(max), [CrossStreet], 126) + CHAR(183) + CONVERT(varchar(max), [CollisionOrder], 126)) as varbinary(16)),
        [OccurrencePrecinctCode] int null,
        [CollisionID] int null,
        [CollisionKey] int null,
        [CollisionOrder] tinyint not null,
        [IntersectionAddress] varchar(321) null,
        [IntersectingStreet] varchar(321) not null,
        [CrossStreet] varchar(321) not null,
        [CollisionVehicleCount] tinyint null,
        [CollisionInjuredCount] tinyint null,
        [CollisionKilledCount] tinyint null
    );
    IF Object_ID('dbo.%Source%_CollisionMetadata_Typed', 'U') IS NOT NULL
    DROP TABLE [dbo].[%Source%_CollisionMetadata_Typed];
    CREATE TABLE [dbo].[%Source%_CollisionMetadata_Typed] (
        _id int not null,
        _file AS metadata_CO_ID, -- keep an alias for backwards compatibility
        metadata_CO_ID int not null,
        metadata_JB_ID int not null,
        _timestamp datetime not null default getdate(),
        [month] varchar(42) null,
        [year] smallint null,
        [notes] varchar(max) null,
        [changedAt] as CAST(dateadd(day, -1,
            dateadd(month, 1,
            cast([year] as char(4)) +
            case left([month], 3)
                when 'Jan' then '01'
                when 'Feb' then '02'
                when 'Mar' then '03'
                when 'Apr' then '04'
                when 'May' then '05'
                when 'Jun' then '06'
                when 'Jul' then '07'
                when 'Aug' then '08'
                when 'Sep' then '09'
                when 'Okt' then '10'
                when 'Nov' then '11'
                when 'Dec' then '12'
            end +
            '01')) AS date) 
    );
    EXEC Stage.metadata._WorkStopping @workId, 'Success';
END TRY
BEGIN CATCH
	SELECT
		@theErrorLine = ERROR_LINE(),
		@theErrorMessage = ERROR_MESSAGE(),
        @theErrorSeverity = ERROR_SEVERITY(),
        @theErrorState = ERROR_STATE();
    EXEC Stage.metadata._WorkStopping
        @WO_ID = @workId, 
        @status = 'Failure', 
        @errorLine = @theErrorLine, 
        @errorMessage = @theErrorMessage;
    -- Propagate the error
    RAISERROR(
        @theErrorMessage,
        @theErrorSeverity,
        @theErrorState
    ); 
END CATCH
END
GO
IF Object_ID('dbo.%Source%_SplitRawIntoTyped', 'P') IS NOT NULL
DROP PROCEDURE [dbo].[%Source%_SplitRawIntoTyped];
GO
--------------------------------------------------------------------------
-- Procedure: %Source%_SplitRawIntoTyped
--
-- This procedure loads data from the 'Split' views into the 'Typed'
-- tables, with the condition that data must conform to the given
-- data types and have no duplicates for defined keys.
--
-- Load: %Source%_Collision_Split into %Source%_Collision_Typed
-- Load: %Source%_CollisionMetadata_Split into %Source%_CollisionMetadata_Typed
--
-- Generated: Fri Mar 11 13:01:07 UTC+0800 2016 by rfrasier
-- From: HKWTR130 in the AD domain
--------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[%Source%_SplitRawIntoTyped] (
    @agentJobId uniqueidentifier = null,
    @agentStepId smallint = null
)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @JB_ID int;
DECLARE @insert int;
DECLARE @updates int;
DECLARE @workId int;
DECLARE @operationsId int;
DECLARE @theErrorLine int;
DECLARE @theErrorMessage varchar(555);
DECLARE @theErrorSeverity int;
DECLARE @theErrorState int;
EXEC Stage.metadata._WorkStarting
    @configurationName = '%Source%', 
    @configurationType = 'Source', 
    @WO_ID = @workId OUTPUT, 
    @name = '%Source%_SplitRawIntoTyped',
    @agentStepId = @agentStepId,
    @agentJobId = @agentJobId
BEGIN TRY
    IF Object_ID('dbo.%Source%_Collision_Typed', 'U') IS NOT NULL
    BEGIN
EXEC Stage.metadata._WorkSourceToTarget
    @OP_ID = @operationsId OUTPUT,
    @WO_ID = @workId, 
    @sourceName = '%Source%_Collision_Split', 
    @targetName = '%Source%_Collision_Typed', 
    @sourceType = 'View', 
    @targetType = 'Table', 
    @sourceCreated = DEFAULT,
    @targetCreated = DEFAULT;
    INSERT INTO [dbo].[%Source%_Collision_Typed] (
        _id,
        metadata_CO_ID,
        metadata_JB_ID,
        [OccurrencePrecinctCode],
        [CollisionID],
        [CollisionKey],
        [CollisionOrder],
        [IntersectionAddress],
        [IntersectingStreet],
        [CrossStreet],
        [CollisionVehicleCount],
        [CollisionInjuredCount],
        [CollisionKilledCount]
    )
    SELECT
        _id,
        metadata_CO_ID,
        metadata_JB_ID,
        [OccurrencePrecinctCode],
        [CollisionID],
        [CollisionKey],
        [CollisionOrder],
        [IntersectionAddress],
        [IntersectingStreet],
        [CrossStreet],
        [CollisionVehicleCount],
        [CollisionInjuredCount],
        [CollisionKilledCount]
    FROM
        [dbo].[%Source%_Collision_Split]
    WHERE
        measureTime_Duplicate = 0
    SET @insert = @insert + @@ROWCOUNT;
    EXEC Stage.metadata._WorkSetInserts @workId, @operationsId, @insert;
    SET @JB_ID = ISNULL((
        SELECT TOP 1
            JB_ID
        FROM
            Stage.metadata.lJB_Job
        WHERE
            JB_AID_Job_AgentJobId = @agentJobId
    ), 0);
    UPDATE [dbo].[%Source%_Collision_Typed]
    SET
      metadata_JB_ID = @JB_ID
    WHERE
      metadata_JB_ID <> @JB_ID;
    SET @updates = @@ROWCOUNT;
    EXEC Stage.metadata._WorkSetUpdates @workId, @operationsId, @updates;
    END
    IF Object_ID('dbo.%Source%_CollisionMetadata_Typed', 'U') IS NOT NULL
    BEGIN
EXEC Stage.metadata._WorkSourceToTarget
    @OP_ID = @operationsId OUTPUT,
    @WO_ID = @workId, 
    @sourceName = '%Source%_CollisionMetadata_Split', 
    @targetName = '%Source%_CollisionMetadata_Typed', 
    @sourceType = 'View', 
    @targetType = 'Table', 
    @sourceCreated = DEFAULT,
    @targetCreated = DEFAULT;
    INSERT INTO [dbo].[%Source%_CollisionMetadata_Typed] (
        _id,
        metadata_CO_ID,
        metadata_JB_ID,
        [month],
        [year],
        [notes]
    )
    SELECT
        _id,
        metadata_CO_ID,
        metadata_JB_ID,
        [month],
        [year],
        [notes]
    FROM
        [dbo].[%Source%_CollisionMetadata_Split]
    WHERE
        [month_Error] is null
    AND
        [year_Error] is null
    AND
        [notes_Error] is null;
    SET @insert = @insert + @@ROWCOUNT;
    EXEC Stage.metadata._WorkSetInserts @workId, @operationsId, @insert;
    SET @JB_ID = ISNULL((
        SELECT TOP 1
            JB_ID
        FROM
            Stage.metadata.lJB_Job
        WHERE
            JB_AID_Job_AgentJobId = @agentJobId
    ), 0);
    UPDATE [dbo].[%Source%_CollisionMetadata_Typed]
    SET
      metadata_JB_ID = @JB_ID
    WHERE
      metadata_JB_ID <> @JB_ID;
    SET @updates = @@ROWCOUNT;
    EXEC Stage.metadata._WorkSetUpdates @workId, @operationsId, @updates;
    END
    EXEC Stage.metadata._WorkStopping @workId, 'Success';
END TRY
BEGIN CATCH
	SELECT
		@theErrorLine = ERROR_LINE(),
		@theErrorMessage = ERROR_MESSAGE(),
        @theErrorSeverity = ERROR_SEVERITY(),
        @theErrorState = ERROR_STATE();
    EXEC Stage.metadata._WorkStopping
        @WO_ID = @workId, 
        @status = 'Failure', 
        @errorLine = @theErrorLine, 
        @errorMessage = @theErrorMessage;
    -- Propagate the error
    RAISERROR(
        @theErrorMessage,
        @theErrorSeverity,
        @theErrorState
    ); 
END CATCH
END
GO
IF Object_ID('dbo.%Source%_AddKeysToTyped', 'P') IS NOT NULL
DROP PROCEDURE [dbo].[%Source%_AddKeysToTyped];
GO
--------------------------------------------------------------------------
-- Procedure: %Source%_AddKeysToTyped
--
-- This procedure adds keys defined in the source xml definition to the 
-- typed staging tables. Keys boost performance when loading is made 
-- using MERGE statements on the target with a search condition that 
-- matches the key composition. Primary keys also guarantee uniquness
-- among its values.
--
-- Table: %Source%_Collision_Typed
-- Key: IntersectingStreet (as primary key)
-- Key: CrossStreet (as primary key)
-- Key: CollisionOrder (as primary key)
--
-- Generated: Fri Mar 11 13:01:07 UTC+0800 2016 by rfrasier
-- From: HKWTR130 in the AD domain
--------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[%Source%_AddKeysToTyped] (
    @agentJobId uniqueidentifier = null,
    @agentStepId smallint = null
)
AS
BEGIN
    SET NOCOUNT ON;
DECLARE @workId int;
DECLARE @operationsId int;
DECLARE @theErrorLine int;
DECLARE @theErrorMessage varchar(555);
DECLARE @theErrorSeverity int;
DECLARE @theErrorState int;
EXEC Stage.metadata._WorkStarting
    @configurationName = '%Source%', 
    @configurationType = 'Source', 
    @WO_ID = @workId OUTPUT, 
    @name = '%Source%_AddKeysToTyped',
    @agentStepId = @agentStepId,
    @agentJobId = @agentJobId
BEGIN TRY
    IF Object_ID('dbo.%Source%_Collision_Typed', 'U') IS NOT NULL
    ALTER TABLE [dbo].[%Source%_Collision_Typed]
    ADD
        CONSTRAINT [dbo_measureTime_%Source%_Collision_Typed] primary key (
            [IntersectingStreet],
            [CrossStreet],
            [CollisionOrder]
        );
    EXEC Stage.metadata._WorkStopping @workId, 'Success';
END TRY
BEGIN CATCH
	SELECT
		@theErrorLine = ERROR_LINE(),
		@theErrorMessage = ERROR_MESSAGE(),
        @theErrorSeverity = ERROR_SEVERITY(),
        @theErrorState = ERROR_STATE();
    EXEC Stage.metadata._WorkStopping
        @WO_ID = @workId, 
        @status = 'Failure', 
        @errorLine = @theErrorLine, 
        @errorMessage = @theErrorMessage;
    -- Propagate the error
    RAISERROR(
        @theErrorMessage,
        @theErrorSeverity,
        @theErrorState
    ); 
END CATCH
END
GO
-- The source definition used when generating the above
DECLARE @xml XML = N'
<source name="%Source%" codepage="ACP" datafiletype="char" fieldterminator="\r\n" rowlength="1000" split="regex" firstrow="1">
	<description>http://www.nyc.gov/html/nypd/html/traffic_reports/motor_vehicle_collision_data.shtml</description>
	<part name="Collision" nulls="" typeCheck="false" keyCheck="true">
        -- this matches the data rows
        SELECT * from %SourceSchema%.%System%_%Source%_Raw WHERE [row] LIKE ''[0-9][0-9][0-9];%''
        <term name="OccurrencePrecinctCode" delimiter=";" format="int"/>
		<term name="CollisionID" pattern="[0-9]{4}([0-9]{9})[^;]*;" format="int"/>
		<term name="CollisionKey" delimiter=";" format="int"/>
		<term name="CollisionOrder" delimiter=";" format="tinyint"/>
		<term name="IntersectionAddress" delimiter=";" format="varchar(321)"/>
		<term name="IntersectingStreet" delimiter=";" format="varchar(321)"/>
		<term name="CrossStreet" delimiter=";" format="varchar(321)"/>
		<term name="CollisionVehicleCount" delimiter=";" format="tinyint"/>
		<term name="CollisionInjuredCount" delimiter=";" format="tinyint"/>
		<term name="CollisionKilledCount" delimiter=";" format="tinyint"/>
		<key name="measureTime" type="primary key">
			<component of="IntersectingStreet"/>
			<component of="CrossStreet"/>
			<component of="CollisionOrder"/>
		</key>
	</part>
	<part name="CollisionMetadata">
        SELECT
          *
        FROM (
          SELECT
            metadata_CO_ID,
						MIN(metadata_JB_ID) as metadata_JB_ID,
            MIN(_id) as _id,
            MIN(_timestamp) as _timestamp
          FROM (
                    SELECT
                        *
                    FROM
                        %SourceSchema%.%System%_%Source%_Raw
                    WHERE
                        [row] NOT LIKE ''[0-9][0-9][0-9];%''
            ) src
          GROUP BY
            metadata_CO_ID
        ) f
        CROSS APPLY (
          SELECT
            [row] + CHAR(183) AS [text()]
          FROM (
                    SELECT
                        *
                    FROM
                        %SourceSchema%.%System%_%Source%_Raw
                    WHERE
                        [row] NOT LIKE ''[0-9][0-9][0-9];%''
            ) src
          WHERE
            src.metadata_CO_ID = f.metadata_CO_ID
          FOR XML PATH('''')
        ) c ([row])
        <term name="month" pattern="(?=.*?(\w+)\s+[0-9]{4})?" format="varchar(42)"/>
		<term name="year" pattern="(?=.*?\w+\s+([0-9]{4}))?" format="smallint"/>
		<calculation name="changedAt" format="date" persisted="false">
            dateadd(day, -1,
            dateadd(month, 1,
            cast([year] as char(4)) +
            case left([month], 3)
                when ''Jan'' then ''01''
                when ''Feb'' then ''02''
                when ''Mar'' then ''03''
                when ''Apr'' then ''04''
                when ''May'' then ''05''
                when ''Jun'' then ''06''
                when ''Jul'' then ''07''
                when ''Aug'' then ''08''
                when ''Sep'' then ''09''
                when ''Okt'' then ''10''
                when ''Nov'' then ''11''
                when ''Dec'' then ''12''
            end +
            ''01''))
        </calculation>
		<term name="notes" pattern="(?=.*?NOTES[^:]*:(.*))?" format="varchar(max)">
            LTRIM(REPLACE([notes], ''�'', '' ''))
        </term>
	</part>
</source>
';
DECLARE @name varchar(255) = @xml.value('/source[1]/@name', 'varchar(255)');
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
        'Source',
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
