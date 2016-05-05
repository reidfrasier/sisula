-- The target definition used when generating the above
DECLARE @xml XML = N'<target name="Traffic" database="%TargetDatabase%">
	<load source="%System%_%Source%_Collision_Typed" target="lST_Street">
		<sql position="before">
        -- preparations can be put here
        </sql>
        select
            StreetName, 
            min(metadata_CO_ID) as metadata_CO_ID
        from (
            select distinct
                IntersectingStreet as StreetName,
                metadata_CO_ID
            from 
                %SourceSchema%.%System%_%Source%_Collision_Typed
            union 
            select distinct
                CrossStreet as StreetName,
                metadata_CO_ID
            from
                %SourceSchema%.%System%_%Source%_Collision_Typed
        ) s
        group by
            StreetName
        <map source="StreetName" target="ST_NAM_Street_Name" as="natural key"/>
		<map source="metadata_CO_ID" target="Metadata_ST" as="metadata"/>
		<sql position="after">
        -- post processing can be put here
        </sql>
	</load>
	<load source="%System%_%Source%_Collision_Typed" target="lIS_Intersection" pass="1">
        select 
            src.IntersectingStreet,
            src.CrossStreet,
            src.metadata_CO_ID,
            stst.IS_ID_of
        from (
            select 
                IntersectingStreet,
                CrossStreet,
                min(metadata_CO_ID) as metadata_CO_ID
            from
                %SourceSchema%.%System%_%Source%_Collision_Typed 
            group by
                IntersectingStreet,
                CrossStreet 
        ) src
        left join
            [%TargetDatabase%].%TargetSchema%.lST_Street st_i
        on
            st_i.ST_NAM_Street_Name = src.IntersectingStreet
        left join
            [%TargetDatabase%].%TargetSchema%.lST_Street st_c
        on
            st_c.ST_NAM_Street_Name = src.CrossStreet
        left join
            [%TargetDatabase%].%TargetSchema%.ST_intersecting_IS_of_ST_crossing stst
        on
            stst.ST_ID_intersecting = st_i.ST_ID
        and
            stst.ST_ID_crossing = st_c.ST_ID 
        <map source="IS_ID_of" target="IS_ID" as="surrogate key"/>
		<map source="metadata_CO_ID" target="Metadata_IS" as="metadata"/>
	</load>
	<load source="%System%_%Source%_Collision_Typed" target="lST_intersecting_IS_of_ST_crossing">
        select
            i.IS_ID_of,
            t.ST_ID_intersecting,
            t.ST_ID_crossing,
            t.metadata_CO_ID
        from (
            select
                i.IS_ID as IS_ID_of,
                row_number() over (order by i.IS_ID) as _rowId
            from
                [%TargetDatabase%].%TargetSchema%.lIS_Intersection i
            left join
                [%TargetDatabase%].%TargetSchema%.ST_intersecting_IS_of_ST_crossing stst
            on
                stst.IS_ID_of = i.IS_ID
        ) i
        join (
            select 
                src.metadata_CO_ID,
                st_i.ST_ID as ST_ID_intersecting,
                st_c.ST_ID as ST_ID_crossing,
                row_number() over (order by st_i.ST_ID, st_c.ST_ID) as _rowId
            from (
                select 
                    IntersectingStreet,
                    CrossStreet,
                    min(metadata_CO_ID) as metadata_CO_ID
                from
                    %SourceSchema%.%System%_%Source%_Collision_Typed 
                group by
                    IntersectingStreet,
                    CrossStreet 
            ) src
            left join
                [%TargetDatabase%].%TargetSchema%.lST_Street st_i
            on
                st_i.ST_NAM_Street_Name = src.IntersectingStreet
            left join
                [%TargetDatabase%].%TargetSchema%.lST_Street st_c
            on
                st_c.ST_NAM_Street_Name = src.CrossStreet
            left join
                [%TargetDatabase%].%TargetSchema%.ST_intersecting_IS_of_ST_crossing stst
            on
                stst.ST_ID_intersecting = st_i.ST_ID
            and
                stst.ST_ID_crossing = st_c.ST_ID 
            where
                stst.IS_ID_of is null
        ) t
        on
            t._rowId = i._rowId
        <map source="ST_ID_intersecting" target="ST_ID_intersecting" as="natural key"/>
		<map source="ST_ID_crossing" target="ST_ID_crossing" as="natural key"/>
		<map source="IS_ID_of" target="IS_ID_of"/>
		<map source="metadata_CO_ID" target="Metadata_ST_intersecting_IS_of_ST_crossing" as="metadata"/>
	</load>
	<load source="%System%_%Source%_Collision_Typed" target="lIS_Intersection" pass="2">
        select
            md.ChangedAt,
            stst.IS_ID_of,
            count(*) as CollisionCount,
            sum(src.CollisionVehicleCount) as CollisionVehicleCount,
            sum(src.CollisionInjuredCount) as CollisionInjuredCount,
            sum(src.CollisionKilledCount) as CollisionKilledCount
        from
            %SourceSchema%.%System%_%Source%_Collision_Typed src
        join
            %SourceSchema%.%System%_%Source%_CollisionMetadata_Typed md
        on
            md.metadata_CO_ID = src.metadata_CO_ID
        join
            [%TargetDatabase%].%TargetSchema%.lST_Street st_i
        on
            st_i.ST_NAM_Street_Name = src.IntersectingStreet
        join
            [%TargetDatabase%].%TargetSchema%.lST_Street st_c
        on
            st_c.ST_NAM_Street_Name = src.CrossStreet
        join
            [%TargetDatabase%].%TargetSchema%.ST_intersecting_IS_of_ST_crossing stst
        on
            stst.ST_ID_intersecting = st_i.ST_ID
        and
            stst.ST_ID_crossing = st_c.ST_ID
        group by
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
        'Target',
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
