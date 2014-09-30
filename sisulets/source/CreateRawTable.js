// Create a raw table suitable for bulk insert
/*~
IF Object_ID('$source.qualified$_CreateRawTable', 'P') IS NOT NULL
DROP PROCEDURE [$source.qualified$_CreateRawTable];
GO

--------------------------------------------------------------------------
-- Procedure: $source.qualified$_CreateRawTable
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
-- _file
-- A number containing the file id, which either points to metadata
-- if its used or is otherwise an incremented number per file.
--
-- _timestamp
-- The time the row was created.
-- 
-- Generated: ${new Date()}$ by $VARIABLES.USERNAME
-- From: $VARIABLES.COMPUTERNAME in the $VARIABLES.USERDOMAIN domain
--------------------------------------------------------------------------
CREATE PROCEDURE [$source.qualified$_CreateRawTable] 
AS
BEGIN
SET NOCOUNT ON;
~*/
beginMetadata(source.qualified + '_CreateRawTable');
/*~
    IF Object_ID('$source.qualified$_Raw', 'U') IS NOT NULL
    DROP TABLE [$source.qualified$_Raw];

    CREATE TABLE [$source.qualified$_Raw] (
        _id int identity(1,1) not null,
        _file int not null default 0,
        _timestamp datetime2(2) not null default sysdatetime(),
        [row] $(source.characterType == 'char')? varchar(max), : nvarchar(max),
        constraint [pk$source.qualified$_Raw] primary key(
            _id asc
        )
    );
~*/
endMetadata();
/*~
END
GO
~*/
