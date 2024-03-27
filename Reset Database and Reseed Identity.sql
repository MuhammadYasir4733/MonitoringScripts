--RESET TABLE
-- TRUE

EXEC sp_MSForEachTable 'DISABLE TRIGGER ALL ON [Schema].[Table]'
GO
EXEC sp_MSForEachTable 'ALTER TABLE [Schema].[Table] NOCHECK CONSTRAINT ALL'
GO
EXEC sp_MSForEachTable 'DELETE FROM [Schema].[Table]'
GO
EXEC sp_MSForEachTable 'ALTER TABLE [Schema].[Table] CHECK CONSTRAINT ALL'
GO
EXEC sp_MSForEachTable 'ENABLE TRIGGER ALL ON [Schema].[Table]'
GO

-- RESET IDENTITY OF TABLE 
DBCC CHECKIDENT ('[Schema].[Table]', RESEED, 0);





-- RESET DATABASE
-- TRUE
EXEC sp_MSForEachTable 'DISABLE TRIGGER ALL ON ?'
GO
EXEC sp_MSForEachTable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'
GO
EXEC sp_MSForEachTable 'DELETE FROM ?'
GO
EXEC sp_MSForEachTable 'ALTER TABLE ? CHECK CONSTRAINT ALL'
GO
EXEC sp_MSForEachTable 'ENABLE TRIGGER ALL ON ?'
GO

-- RESET IDENTITY OF ALL TABLES
EXEC sp_MSForEachTable 'DBCC CHECKIDENT("?", RESEED,0)'





DECLARE @SchemaName NVARCHAR(128)
DECLARE @TableName NVARCHAR(128)
DECLARE @Sql NVARCHAR(MAX)

DECLARE table_cursor CURSOR FOR
SELECT t.name, s.name
FROM sys.tables AS t
INNER JOIN sys.columns AS c ON t.object_id = c.object_id
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
WHERE c.is_identity = 1
    --AND t.name <> 'WorkerScan' -- Exclude the WorkerScan table

OPEN table_cursor

FETCH NEXT FROM table_cursor INTO @TableName, @SchemaName

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @Sql = N'DBCC CHECKIDENT (''' + @SchemaName + '.' + @TableName + ''', RESEED);'
	SELECT @Sql
    --EXEC sp_executesql @Sql

    FETCH NEXT FROM table_cursor INTO @TableName, @SchemaName
END

CLOSE table_cursor
DEALLOCATE table_cursor



