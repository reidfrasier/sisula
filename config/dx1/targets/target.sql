USE Development;
GO
IF Object_ID('source.lST_Street__WQ_Dataexplorers_Collision_Typed', 'P') IS NOT NULL
DROP PROCEDURE [source].[lST_Street__WQ_Dataexplorers_Collision_Typed];
GO
--------------------------------------------------------------------------
-- Procedure: lST_Street__WQ_Dataexplorers_Collision_Typed
-- Source: WQ_Dataexplorers_Collision_Typed
-- Target: lST_Street
--
-- Map: StreetName to ST_NAM_Street_Name (as natural key)
-- Map: metadata_CO_ID to Metadata_ST (as metadata)
--
-- Generated: Tue Apr 19 13:30:00 UTC+0800 2016 by rfrasier
-- From: PWSHKDWQRF001 in the AD domain
--------------------------------------------------------------------------
CREATE PROCEDURE [source].[lST_Street__WQ_Dataexplorers_Collision_Typed] (
    @agentJobId uniqueidentifier = null,
    @agentStepId smallint = null
)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @inserts int;
DECLARE @updates int;
DECLARE @deletes int;
DECLARE @actions TABLE (
    [action] char(1) not null
);
DECLARE @workId int;
DECLARE @operationsId int;
DECLARE @theErrorLine int;
DECLARE @theErrorMessage varchar(555);
DECLARE @theErrorSeverity int;
DECLARE @theErrorState int;
EXEC Development.metadata._WorkStarting
    @configurationName = 'Traffic', 
    @configurationType = 'Target', 
    @WO_ID = @workId OUTPUT, 
    @name = 'lST_Street__WQ_Dataexplorers_Collision_Typed',
    @agentStepId = @agentStepId,
    @agentJobId = @agentJobId
BEGIN TRY
EXEC Development.metadata._WorkSourceToTarget
    @OP_ID = @operationsId OUTPUT,
    @WO_ID = @workId, 
    @sourceName = 'WQ_Dataexplorers_Collision_Typed', 
    @targetName = 'lST_Street', 
    @sourceType = 'Table', 
    @targetType = 'Table', 
    @sourceCreated = DEFAULT,
    @targetCreated = DEFAULT;
    -- Preparations before the merge -----------------
        -- preparations can be put here
    -- Perform the actual merge ----------------------
    MERGE INTO [Development].[target].[lST_Street] AS t
    USING (
        SELECT
            StreetName,
            MIN(metadata_CO_ID) AS metadata_CO_ID
        FROM (
            SELECT DISTINCT
                IntersectingStreet AS StreetName,
                metadata_CO_ID
            FROM
                source.WQ_Dataexplorers_Collision_Typed
            UNION
            SELECT DISTINCT
                CrossStreet AS StreetName,
                metadata_CO_ID
            FROM
                source.WQ_Dataexplorers_Collision_Typed
        ) s
        GROUP BY
            StreetName
    ) AS s
    ON (
        s.[StreetName] = t.[ST_NAM_Street_Name]
    )
    WHEN NOT MATCHED THEN INSERT (
        [ST_NAM_Street_Name],
        [Metadata_ST]
    )
    VALUES (
        s.[StreetName],
        s.[metadata_CO_ID]
    )
    OUTPUT
        LEFT($action, 1) INTO @actions;
    SELECT
        @inserts = NULLIF(COUNT(CASE WHEN [action] = 'I' THEN 1 END), 0),
        @updates = NULLIF(COUNT(CASE WHEN [action] = 'U' THEN 1 END), 0),
        @deletes = NULLIF(COUNT(CASE WHEN [action] = 'D' THEN 1 END), 0)
    FROM
        @actions;
    EXEC Development.metadata._WorkSetInserts @workId, @operationsId, @inserts;
    EXEC Development.metadata._WorkSetUpdates @workId, @operationsId, @updates;
    EXEC Development.metadata._WorkSetDeletes @workId, @operationsId, @deletes;
    -- Post processing after the merge ---------------
        -- post processing can be put here
    EXEC Development.metadata._WorkStopping @workId, 'Success';
END TRY
BEGIN CATCH
	SELECT
		@theErrorLine = ERROR_LINE(),
		@theErrorMessage = ERROR_MESSAGE(),
        @theErrorSeverity = ERROR_SEVERITY(),
        @theErrorState = ERROR_STATE();
    EXEC Development.metadata._WorkStopping
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
IF Object_ID('source.lIS_Intersection__WQ_Dataexplorers_Collision_Typed__1', 'P') IS NOT NULL
DROP PROCEDURE [source].[lIS_Intersection__WQ_Dataexplorers_Collision_Typed__1];
GO
--------------------------------------------------------------------------
-- Procedure: lIS_Intersection__WQ_Dataexplorers_Collision_Typed__1
-- Source: WQ_Dataexplorers_Collision_Typed
-- Target: lIS_Intersection
--
-- Map: IS_ID_of to IS_ID (as surrogate key)
-- Map: metadata_CO_ID to Metadata_IS (as metadata)
--
-- Generated: Tue Apr 19 13:30:00 UTC+0800 2016 by rfrasier
-- From: PWSHKDWQRF001 in the AD domain
--------------------------------------------------------------------------
CREATE PROCEDURE [source].[lIS_Intersection__WQ_Dataexplorers_Collision_Typed__1] (
    @agentJobId uniqueidentifier = null,
    @agentStepId smallint = null
)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @inserts int;
DECLARE @updates int;
DECLARE @deletes int;
DECLARE @actions TABLE (
    [action] char(1) not null
);
DECLARE @workId int;
DECLARE @operationsId int;
DECLARE @theErrorLine int;
DECLARE @theErrorMessage varchar(555);
DECLARE @theErrorSeverity int;
DECLARE @theErrorState int;
EXEC Development.metadata._WorkStarting
    @configurationName = 'Traffic', 
    @configurationType = 'Target', 
    @WO_ID = @workId OUTPUT, 
    @name = 'lIS_Intersection__WQ_Dataexplorers_Collision_Typed__1',
    @agentStepId = @agentStepId,
    @agentJobId = @agentJobId
BEGIN TRY
EXEC Development.metadata._WorkSourceToTarget
    @OP_ID = @operationsId OUTPUT,
    @WO_ID = @workId, 
    @sourceName = 'WQ_Dataexplorers_Collision_Typed', 
    @targetName = 'lIS_Intersection', 
    @sourceType = 'Table', 
    @targetType = 'Table', 
    @sourceCreated = DEFAULT,
    @targetCreated = DEFAULT;
    -- Perform the actual merge ----------------------
    MERGE INTO [Development].[target].[lIS_Intersection] AS t
    USING (
        SELECT
            src.IntersectingStreet,
            src.CrossStreet,
            src.metadata_CO_ID,
            stst.IS_ID_of
        FROM (
            SELECT
                IntersectingStreet,
                CrossStreet,
                MIN(metadata_CO_ID) AS metadata_CO_ID
            FROM
                source.WQ_Dataexplorers_Collision_Typed
            GROUP BY
                IntersectingStreet,
                CrossStreet
        ) src
        LEFT JOIN
            [Development].target.lST_Street st_i
        ON
            st_i.ST_NAM_Street_Name = src.IntersectingStreet
        LEFT JOIN
            [Development].target.lST_Street st_c
        ON
            st_c.ST_NAM_Street_Name = src.CrossStreet
        LEFT JOIN
            [Development].target.ST_intersecting_IS_of_ST_crossing stst
        ON
            stst.ST_ID_intersecting = st_i.ST_ID
        AND
            stst.ST_ID_crossing = st_c.ST_ID
    ) AS s
    ON (
        s.[IS_ID_of] = t.[IS_ID]
    )
    WHEN NOT MATCHED THEN INSERT (
        [Metadata_IS]
    )
    VALUES (
        s.[metadata_CO_ID]
    )
    OUTPUT
        LEFT($action, 1) INTO @actions;
    SELECT
        @inserts = NULLIF(COUNT(CASE WHEN [action] = 'I' THEN 1 END), 0),
        @updates = NULLIF(COUNT(CASE WHEN [action] = 'U' THEN 1 END), 0),
        @deletes = NULLIF(COUNT(CASE WHEN [action] = 'D' THEN 1 END), 0)
    FROM
        @actions;
    EXEC Development.metadata._WorkSetInserts @workId, @operationsId, @inserts;
    EXEC Development.metadata._WorkSetUpdates @workId, @operationsId, @updates;
    EXEC Development.metadata._WorkSetDeletes @workId, @operationsId, @deletes;
    EXEC Development.metadata._WorkStopping @workId, 'Success';
END TRY
BEGIN CATCH
	SELECT
		@theErrorLine = ERROR_LINE(),
		@theErrorMessage = ERROR_MESSAGE(),
        @theErrorSeverity = ERROR_SEVERITY(),
        @theErrorState = ERROR_STATE();
    EXEC Development.metadata._WorkStopping
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
IF Object_ID('source.lST_intersecting_IS_of_ST_crossing__WQ_Dataexplorers_Collision_Typed', 'P') IS NOT NULL
DROP PROCEDURE [source].[lST_intersecting_IS_of_ST_crossing__WQ_Dataexplorers_Collision_Typed];
GO
--------------------------------------------------------------------------
-- Procedure: lST_intersecting_IS_of_ST_crossing__WQ_Dataexplorers_Collision_Typed
-- Source: WQ_Dataexplorers_Collision_Typed
-- Target: lST_intersecting_IS_of_ST_crossing
--
-- Map: ST_ID_intersecting to ST_ID_intersecting (as natural key)
-- Map: ST_ID_crossing to ST_ID_crossing (as natural key)
-- Map: IS_ID_of to IS_ID_of 
-- Map: metadata_CO_ID to Metadata_ST_intersecting_IS_of_ST_crossing (as metadata)
--
-- Generated: Tue Apr 19 13:30:00 UTC+0800 2016 by rfrasier
-- From: PWSHKDWQRF001 in the AD domain
--------------------------------------------------------------------------
CREATE PROCEDURE [source].[lST_intersecting_IS_of_ST_crossing__WQ_Dataexplorers_Collision_Typed] (
    @agentJobId uniqueidentifier = null,
    @agentStepId smallint = null
)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @inserts int;
DECLARE @updates int;
DECLARE @deletes int;
DECLARE @actions TABLE (
    [action] char(1) not null
);
DECLARE @workId int;
DECLARE @operationsId int;
DECLARE @theErrorLine int;
DECLARE @theErrorMessage varchar(555);
DECLARE @theErrorSeverity int;
DECLARE @theErrorState int;
EXEC Development.metadata._WorkStarting
    @configurationName = 'Traffic', 
    @configurationType = 'Target', 
    @WO_ID = @workId OUTPUT, 
    @name = 'lST_intersecting_IS_of_ST_crossing__WQ_Dataexplorers_Collision_Typed',
    @agentStepId = @agentStepId,
    @agentJobId = @agentJobId
BEGIN TRY
EXEC Development.metadata._WorkSourceToTarget
    @OP_ID = @operationsId OUTPUT,
    @WO_ID = @workId, 
    @sourceName = 'WQ_Dataexplorers_Collision_Typed', 
    @targetName = 'lST_intersecting_IS_of_ST_crossing', 
    @sourceType = 'Table', 
    @targetType = 'Table', 
    @sourceCreated = DEFAULT,
    @targetCreated = DEFAULT;
    -- Perform the actual merge ----------------------
    MERGE INTO [Development].[target].[lST_intersecting_IS_of_ST_crossing] AS t
    USING (
        SELECT
            i.IS_ID_of,
            t.ST_ID_intersecting,
            t.ST_ID_crossing,
            t.metadata_CO_ID
        FROM (
            SELECT
                i.IS_ID AS IS_ID_of,
                row_number() over (order by i.IS_ID) AS _rowId
            FROM
                [Development].target.lIS_Intersection i
            LEFT JOIN
                [Development].target.ST_intersecting_IS_of_ST_crossing stst
            ON
                stst.IS_ID_of = i.IS_ID
        ) i
        JOIN (
            SELECT
                src.metadata_CO_ID,
                st_i.ST_ID AS ST_ID_intersecting,
                st_c.ST_ID AS ST_ID_crossing,
                ROW_NUMBER() OVER (ORDER BY st_i.ST_ID, st_c.ST_ID) AS _rowId
            FROM (
                SELECT
                    IntersectingStreet,
                    CrossStreet,
                    min(metadata_CO_ID) AS metadata_CO_ID
                FROM
                    source.WQ_Dataexplorers_Collision_Typed
                GROUP BY
                    IntersectingStreet,
                    CrossStreet
            ) src
            LEFT JOIN
                [Development].target.lST_Street st_i
            ON
                st_i.ST_NAM_Street_Name = src.IntersectingStreet
            LEFT JOIN
                [Development].target.lST_Street st_c
            ON
                st_c.ST_NAM_Street_Name = src.CrossStreet
            LEFT JOIN
                [Development].target.ST_intersecting_IS_of_ST_crossing stst
            ON
                stst.ST_ID_intersecting = st_i.ST_ID
            AND
                stst.ST_ID_crossing = st_c.ST_ID
            WHERE
                stst.IS_ID_of is null
        ) t
        ON
            t._rowId = i._rowId
    ) AS s
    ON (
        s.[ST_ID_intersecting] = t.[ST_ID_intersecting]
    AND
        s.[ST_ID_crossing] = t.[ST_ID_crossing]
    )
    WHEN NOT MATCHED THEN INSERT (
        [ST_ID_intersecting],
        [ST_ID_crossing],
        [IS_ID_of],
        [Metadata_ST_intersecting_IS_of_ST_crossing]
    )
    VALUES (
        s.[ST_ID_intersecting],
        s.[ST_ID_crossing],
        s.[IS_ID_of],
        s.[metadata_CO_ID]
    )
    WHEN MATCHED AND (
        (t.[IS_ID_of] is null OR s.[IS_ID_of] <> t.[IS_ID_of])
    ) 
    THEN UPDATE
    SET
        t.[IS_ID_of] = s.[IS_ID_of],
        t.[Metadata_ST_intersecting_IS_of_ST_crossing] = s.[metadata_CO_ID]
    OUTPUT
        LEFT($action, 1) INTO @actions;
    SELECT
        @inserts = NULLIF(COUNT(CASE WHEN [action] = 'I' THEN 1 END), 0),
        @updates = NULLIF(COUNT(CASE WHEN [action] = 'U' THEN 1 END), 0),
        @deletes = NULLIF(COUNT(CASE WHEN [action] = 'D' THEN 1 END), 0)
    FROM
        @actions;
    EXEC Development.metadata._WorkSetInserts @workId, @operationsId, @inserts;
    EXEC Development.metadata._WorkSetUpdates @workId, @operationsId, @updates;
    EXEC Development.metadata._WorkSetDeletes @workId, @operationsId, @deletes;
    EXEC Development.metadata._WorkStopping @workId, 'Success';
END TRY
BEGIN CATCH
	SELECT
		@theErrorLine = ERROR_LINE(),
		@theErrorMessage = ERROR_MESSAGE(),
        @theErrorSeverity = ERROR_SEVERITY(),
        @theErrorState = ERROR_STATE();
    EXEC Development.metadata._WorkStopping
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
IF Object_ID('source.lIS_Intersection__WQ_Dataexplorers_Collision_Typed__2', 'P') IS NOT NULL
DROP PROCEDURE [source].[lIS_Intersection__WQ_Dataexplorers_Collision_Typed__2];
GO
--------------------------------------------------------------------------
-- Procedure: lIS_Intersection__WQ_Dataexplorers_Collision_Typed__2
-- Source: WQ_Dataexplorers_Collision_Typed
-- Target: lIS_Intersection
--
-- Map: IS_ID_of to IS_ID (as surrogate key)
-- Map: CollisionCount to IS_COL_Intersection_CollisionCount 
-- Map: ChangedAt to IS_COL_ChangedAt 
-- Map: CollisionVehicleCount to IS_VEH_Intersection_VehicleCount 
-- Map: ChangedAt to IS_VEH_ChangedAt 
-- Map: CollisionInjuredCount to IS_INJ_Intersection_InjuredCount 
-- Map: ChangedAt to IS_INJ_ChangedAt 
-- Map: CollisionKilledCount to IS_KIL_Intersection_KilledCount 
-- Map: ChangedAt to IS_KIL_ChangedAt 
--
-- Generated: Tue Apr 19 13:30:00 UTC+0800 2016 by rfrasier
-- From: PWSHKDWQRF001 in the AD domain
--------------------------------------------------------------------------
CREATE PROCEDURE [source].[lIS_Intersection__WQ_Dataexplorers_Collision_Typed__2] (
    @agentJobId uniqueidentifier = null,
    @agentStepId smallint = null
)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @inserts int;
DECLARE @updates int;
DECLARE @deletes int;
DECLARE @actions TABLE (
    [action] char(1) not null
);
DECLARE @workId int;
DECLARE @operationsId int;
DECLARE @theErrorLine int;
DECLARE @theErrorMessage varchar(555);
DECLARE @theErrorSeverity int;
DECLARE @theErrorState int;
EXEC Development.metadata._WorkStarting
    @configurationName = 'Traffic', 
    @configurationType = 'Target', 
    @WO_ID = @workId OUTPUT, 
    @name = 'lIS_Intersection__WQ_Dataexplorers_Collision_Typed__2',
    @agentStepId = @agentStepId,
    @agentJobId = @agentJobId
BEGIN TRY
EXEC Development.metadata._WorkSourceToTarget
    @OP_ID = @operationsId OUTPUT,
    @WO_ID = @workId, 
    @sourceName = 'WQ_Dataexplorers_Collision_Typed', 
    @targetName = 'lIS_Intersection', 
    @sourceType = 'Table', 
    @targetType = 'Table', 
    @sourceCreated = DEFAULT,
    @targetCreated = DEFAULT;
    -- Perform the actual merge ----------------------
    MERGE INTO [Development].[target].[lIS_Intersection] AS t
    USING (
        SELECT
            md.ChangedAt,
            stst.IS_ID_of,
            COUNT(*) AS CollisionCount,
            SUM(src.CollisionVehicleCount) AS CollisionVehicleCount,
            SUM(src.CollisionInjuredCount) AS CollisionInjuredCount,
            SUM(src.CollisionKilledCount) AS CollisionKilledCount
        FROM
            source.WQ_Dataexplorers_Collision_Typed src
        JOIN
            source.WQ_Dataexplorers_CollisionMetadata_Typed md
        ON
            md.metadata_CO_ID = src.metadata_CO_ID
        JOIN
            [Development].target.lST_Street st_i
        ON
            st_i.ST_NAM_Street_Name = src.IntersectingStreet
        JOIN
            [Development].target.lST_Street st_c
        ON
            st_c.ST_NAM_Street_Name = src.CrossStreet
        JOIN
            [Development].target.ST_intersecting_IS_of_ST_crossing stst
        ON
            stst.ST_ID_intersecting = st_i.ST_ID
        AND
            stst.ST_ID_crossing = st_c.ST_ID
        GROUP BY
            md.ChangedAt,
            stst.IS_ID_of
    ) AS s
    ON (
        s.[IS_ID_of] = t.[IS_ID]
    )
    WHEN NOT MATCHED THEN INSERT (
        [IS_COL_Intersection_CollisionCount],
        [IS_COL_ChangedAt],
        [IS_VEH_Intersection_VehicleCount],
        [IS_VEH_ChangedAt],
        [IS_INJ_Intersection_InjuredCount],
        [IS_INJ_ChangedAt],
        [IS_KIL_Intersection_KilledCount],
        [IS_KIL_ChangedAt]
    )
    VALUES (
        s.[CollisionCount],
        s.[ChangedAt],
        s.[CollisionVehicleCount],
        s.[ChangedAt],
        s.[CollisionInjuredCount],
        s.[ChangedAt],
        s.[CollisionKilledCount],
        s.[ChangedAt]
    )
    WHEN MATCHED AND (
        (t.[IS_COL_Intersection_CollisionCount] is null OR s.[CollisionCount] <> t.[IS_COL_Intersection_CollisionCount])
    OR 
        (t.[IS_COL_ChangedAt] is null OR s.[ChangedAt] <> t.[IS_COL_ChangedAt])
    OR 
        (t.[IS_VEH_Intersection_VehicleCount] is null OR s.[CollisionVehicleCount] <> t.[IS_VEH_Intersection_VehicleCount])
    OR 
        (t.[IS_VEH_ChangedAt] is null OR s.[ChangedAt] <> t.[IS_VEH_ChangedAt])
    OR 
        (t.[IS_INJ_Intersection_InjuredCount] is null OR s.[CollisionInjuredCount] <> t.[IS_INJ_Intersection_InjuredCount])
    OR 
        (t.[IS_INJ_ChangedAt] is null OR s.[ChangedAt] <> t.[IS_INJ_ChangedAt])
    OR 
        (t.[IS_KIL_Intersection_KilledCount] is null OR s.[CollisionKilledCount] <> t.[IS_KIL_Intersection_KilledCount])
    OR 
        (t.[IS_KIL_ChangedAt] is null OR s.[ChangedAt] <> t.[IS_KIL_ChangedAt])
    ) 
    THEN UPDATE
    SET
        t.[IS_COL_Intersection_CollisionCount] = s.[CollisionCount],
        t.[IS_COL_ChangedAt] = s.[ChangedAt],
        t.[IS_VEH_Intersection_VehicleCount] = s.[CollisionVehicleCount],
        t.[IS_VEH_ChangedAt] = s.[ChangedAt],
        t.[IS_INJ_Intersection_InjuredCount] = s.[CollisionInjuredCount],
        t.[IS_INJ_ChangedAt] = s.[ChangedAt],
        t.[IS_KIL_Intersection_KilledCount] = s.[CollisionKilledCount],
        t.[IS_KIL_ChangedAt] = s.[ChangedAt]
    OUTPUT
        LEFT($action, 1) INTO @actions;
    SELECT
        @inserts = NULLIF(COUNT(CASE WHEN [action] = 'I' THEN 1 END), 0),
        @updates = NULLIF(COUNT(CASE WHEN [action] = 'U' THEN 1 END), 0),
        @deletes = NULLIF(COUNT(CASE WHEN [action] = 'D' THEN 1 END), 0)
    FROM
        @actions;
    EXEC Development.metadata._WorkSetInserts @workId, @operationsId, @inserts;
    EXEC Development.metadata._WorkSetUpdates @workId, @operationsId, @updates;
    EXEC Development.metadata._WorkSetDeletes @workId, @operationsId, @deletes;
    EXEC Development.metadata._WorkStopping @workId, 'Success';
END TRY
BEGIN CATCH
	SELECT
		@theErrorLine = ERROR_LINE(),
		@theErrorMessage = ERROR_MESSAGE(),
        @theErrorSeverity = ERROR_SEVERITY(),
        @theErrorState = ERROR_STATE();
    EXEC Development.metadata._WorkStopping
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
-- The target definition used when generating the above
DECLARE @xml XML = N'<target name="Traffic" database="Development">
	<load source="WQ_Dataexplorers_Collision_Typed" target="lST_Street">
		<sql position="before">
        -- preparations can be put here
        </sql>
        SELECT
            StreetName,
            MIN(metadata_CO_ID) AS metadata_CO_ID
        FROM (
            SELECT DISTINCT
                IntersectingStreet AS StreetName,
                metadata_CO_ID
            FROM
                source.WQ_Dataexplorers_Collision_Typed
            UNION
            SELECT DISTINCT
                CrossStreet AS StreetName,
                metadata_CO_ID
            FROM
                source.WQ_Dataexplorers_Collision_Typed
        ) s
        GROUP BY
            StreetName
        <map source="StreetName" target="ST_NAM_Street_Name" as="natural key"/>
		<map source="metadata_CO_ID" target="Metadata_ST" as="metadata"/>
		<sql position="after">
        -- post processing can be put here
        </sql>
	</load>
	<load source="WQ_Dataexplorers_Collision_Typed" target="lIS_Intersection" pass="1">
        SELECT
            src.IntersectingStreet,
            src.CrossStreet,
            src.metadata_CO_ID,
            stst.IS_ID_of
        FROM (
            SELECT
                IntersectingStreet,
                CrossStreet,
                MIN(metadata_CO_ID) AS metadata_CO_ID
            FROM
                source.WQ_Dataexplorers_Collision_Typed
            GROUP BY
                IntersectingStreet,
                CrossStreet
        ) src
        LEFT JOIN
            [Development].target.lST_Street st_i
        ON
            st_i.ST_NAM_Street_Name = src.IntersectingStreet
        LEFT JOIN
            [Development].target.lST_Street st_c
        ON
            st_c.ST_NAM_Street_Name = src.CrossStreet
        LEFT JOIN
            [Development].target.ST_intersecting_IS_of_ST_crossing stst
        ON
            stst.ST_ID_intersecting = st_i.ST_ID
        AND
            stst.ST_ID_crossing = st_c.ST_ID
        <map source="IS_ID_of" target="IS_ID" as="surrogate key"/>
		<map source="metadata_CO_ID" target="Metadata_IS" as="metadata"/>
	</load>
	<load source="WQ_Dataexplorers_Collision_Typed" target="lST_intersecting_IS_of_ST_crossing">
        SELECT
            i.IS_ID_of,
            t.ST_ID_intersecting,
            t.ST_ID_crossing,
            t.metadata_CO_ID
        FROM (
            SELECT
                i.IS_ID AS IS_ID_of,
                row_number() over (order by i.IS_ID) AS _rowId
            FROM
                [Development].target.lIS_Intersection i
            LEFT JOIN
                [Development].target.ST_intersecting_IS_of_ST_crossing stst
            ON
                stst.IS_ID_of = i.IS_ID
        ) i
        JOIN (
            SELECT
                src.metadata_CO_ID,
                st_i.ST_ID AS ST_ID_intersecting,
                st_c.ST_ID AS ST_ID_crossing,
                ROW_NUMBER() OVER (ORDER BY st_i.ST_ID, st_c.ST_ID) AS _rowId
            FROM (
                SELECT
                    IntersectingStreet,
                    CrossStreet,
                    min(metadata_CO_ID) AS metadata_CO_ID
                FROM
                    source.WQ_Dataexplorers_Collision_Typed
                GROUP BY
                    IntersectingStreet,
                    CrossStreet
            ) src
            LEFT JOIN
                [Development].target.lST_Street st_i
            ON
                st_i.ST_NAM_Street_Name = src.IntersectingStreet
            LEFT JOIN
                [Development].target.lST_Street st_c
            ON
                st_c.ST_NAM_Street_Name = src.CrossStreet
            LEFT JOIN
                [Development].target.ST_intersecting_IS_of_ST_crossing stst
            ON
                stst.ST_ID_intersecting = st_i.ST_ID
            AND
                stst.ST_ID_crossing = st_c.ST_ID
            WHERE
                stst.IS_ID_of is null
        ) t
        ON
            t._rowId = i._rowId
        <map source="ST_ID_intersecting" target="ST_ID_intersecting" as="natural key"/>
		<map source="ST_ID_crossing" target="ST_ID_crossing" as="natural key"/>
		<map source="IS_ID_of" target="IS_ID_of"/>
		<map source="metadata_CO_ID" target="Metadata_ST_intersecting_IS_of_ST_crossing" as="metadata"/>
	</load>
	<load source="WQ_Dataexplorers_Collision_Typed" target="lIS_Intersection" pass="2">
        SELECT
            md.ChangedAt,
            stst.IS_ID_of,
            COUNT(*) AS CollisionCount,
            SUM(src.CollisionVehicleCount) AS CollisionVehicleCount,
            SUM(src.CollisionInjuredCount) AS CollisionInjuredCount,
            SUM(src.CollisionKilledCount) AS CollisionKilledCount
        FROM
            source.WQ_Dataexplorers_Collision_Typed src
        JOIN
            source.WQ_Dataexplorers_CollisionMetadata_Typed md
        ON
            md.metadata_CO_ID = src.metadata_CO_ID
        JOIN
            [Development].target.lST_Street st_i
        ON
            st_i.ST_NAM_Street_Name = src.IntersectingStreet
        JOIN
            [Development].target.lST_Street st_c
        ON
            st_c.ST_NAM_Street_Name = src.CrossStreet
        JOIN
            [Development].target.ST_intersecting_IS_of_ST_crossing stst
        ON
            stst.ST_ID_intersecting = st_i.ST_ID
        AND
            stst.ST_ID_crossing = st_c.ST_ID
        GROUP BY
            md.ChangedAt,
            stst.IS_ID_of
        <map source="IS_ID_of" target="IS_ID" as="surrogate key"/>
		<map source="CollisionCount" target="IS_COL_Intersection_CollisionCount"/>
		<map source="ChangedAt" target="IS_COL_ChangedAt"/>
		<map source="CollisionVehicleCount" target="IS_VEH_Intersection_VehicleCount"/>
		<map source="ChangedAt" target="IS_VEH_ChangedAt"/>
		<map source="CollisionInjuredCount" target="IS_INJ_Intersection_InjuredCount"/>
		<map source="ChangedAt" target="IS_INJ_ChangedAt"/>
		<map source="CollisionKilledCount" target="IS_KIL_Intersection_KilledCount"/>
		<map source="ChangedAt" target="IS_KIL_ChangedAt"/>
	</load>
</target>
';
DECLARE @name varchar(255) = @xml.value('/target[1]/@name', 'varchar(255)');
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
        'Target',
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
