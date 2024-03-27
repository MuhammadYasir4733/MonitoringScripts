USE ActiveSooperWizerNCL;
GO

DECLARE @TableName NVARCHAR(128)
DECLARE @SQL NVARCHAR(MAX)

DECLARE TableCursor CURSOR FOR
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='dbo' AND TABLE_NAME NOT IN (
'DelEndLineSession','DelEndLineFaultLog','Hour_Dim','JobStatus','MachineIntegration',
'IncentiveMeasure','MachineSequenceLog','PieceStaus','WorkerIncentive','DateTab'
)

OPEN TableCursor

FETCH NEXT FROM TableCursor INTO @TableName

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = 'DROP TABLE ' + @TableName
    EXEC sp_executesql @SQL

    FETCH NEXT FROM TableCursor INTO @TableName
END

CLOSE TableCursor
DEALLOCATE TableCursor


-- =============================================================================================================================================================================================
-- =============================================================================================================================================================================================

USE ActiveSooperWizerNCL;
GO

DECLARE @TableName NVARCHAR(128)
DECLARE @SQL NVARCHAR(MAX)

DECLARE TableCursor CURSOR FOR
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='auth' 

OPEN TableCursor

FETCH NEXT FROM TableCursor INTO @TableName

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = 'DROP TABLE ' + @TableName
    EXEC sp_executesql @SQL

    FETCH NEXT FROM TableCursor INTO @TableName
END

CLOSE TableCursor
DEALLOCATE TableCursor





-- =============================================================================================================================================================================================
-- =============================================================================================================================================================================================


SELECT s.name
FROM sys.schemas s
WHERE s.principal_id = USER_ID('keycloak');

ALTER AUTHORIZATION ON SCHEMA::auth TO dbo;

DROP USER [keycloak];
GO

-- =============================================================================================================================================================================================
-- =============================================================================================================================================================================================



CREATE USER keycloak FOR LOGIN keycloak;
GO

ALTER ROLE db_owner ADD MEMBER keycloak;
GO

DROP SCHEMA auth;
GO

CREATE SCHEMA auth AUTHORIZATION keycloak;
GO

ALTER USER keycloak WITH DEFAULT_SCHEMA = auth;
GO

SELECT SCHEMA_NAME() EXECUTE AS USER='keycloak';
GO