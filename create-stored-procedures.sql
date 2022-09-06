---------------------
-- Procedure to Drop schema along with all the underlying tables
---------------------
CREATE PROCEDURE drop_schema_cascade @schema_name SYSNAME
AS BEGIN
    IF (0 <> (SELECT COUNT(*) FROM sys.schemas WHERE name = @schema_name))
    BEGIN
        -- Get the external table names to delete into a temp table
        CREATE TABLE #external_tables(NAME VARCHAR(200))
        DECLARE @external_table_names_sql NVARCHAR(200) = N'INSERT INTO #external_tables (NAME) SELECT a.name from sys.external_tables a, sys.schemas b WHERE a.schema_id=b.schema_id AND b.name= '''+ @schema_name + ''''

        PRINT 'External tables fetch sql: '+@external_table_names_sql
        PRINT CHAR(13)
        EXEC sp_executesql @external_table_names_sql

        -- Variables needed for external table deletion
        DECLARE @table_deletion_sql NVARCHAR(200)
        DECLARE @table_to_delete VARCHAR(100)
        DECLARE @counter int = 1
        DECLARE @recordCount int = (SELECT COUNT(1) from #external_tables)    

        -- Loop through records in Temp table (external table names) and delete them
        WHILE @counter <= @recordCount 
        BEGIN  
            SET @table_to_delete = (SELECT [name]
            FROM(SELECT *, ROW_NUMBER() OVER(ORDER BY [name]) AS RN
            FROM #external_tables) as T
            WHERE RN = @counter)

            PRINT 'Deleting external table: '+ @table_to_delete
            PRINT CHAR(13)
            SET @table_deletion_sql = N'DROP EXTERNAL TABLE '+ @schema_name + '.' + @table_to_delete
            EXEC sp_executesql @table_deletion_sql

            SET @counter = @counter + 1
        END

        -- Finally, drop the schema
        PRINT 'Deleting Schema: '+ @schema_name
        PRINT CHAR(13)
        DECLARE @drop_schema_sql NVARCHAR(200) = 'DROP SCHEMA '+@schema_name
        EXEC sp_executesql @drop_schema_sql
    END
END
GO

---------------------
-- Procedure to check and create schema - if it does not exist
---------------------
CREATE PROCEDURE create_schema @schema_name SYSNAME
AS BEGIN
    IF (0 = (SELECT COUNT(*) FROM sys.schemas WHERE name = @schema_name))
    BEGIN
        print('Creating shema:'+@schema_name)
        print(CHAR(13))
        DECLARE @create_stmt NVARCHAR(200) = N'CREATE SCHEMA '+@schema_name
        EXEC sp_executesql @tsql = @create_stmt;
    END
    ELSE
    BEGIN
        print('Schema: '+@schema_name+' already exists:')
        print(CHAR(13))
    END
END
GO

---------------------
-- Procedure to check and create external table - if it does not exist
---------------------
CREATE PROCEDURE create_external_table @schema_name SYSNAME, @table_name SYSNAME, @table_creation_query NVARCHAR(max)
AS BEGIN
    IF (0 = (SELECT COUNT(a.name) from sys.external_tables a, sys.schemas b WHERE a.schema_id=b.schema_id AND b.name = @schema_name AND a.name=@table_name))
    BEGIN
        print('Creating external table:'+@table_name)
        print(CHAR(13))
        EXEC sp_executesql @tsql = @table_creation_query;
    END
    ELSE
    BEGIN
        print('External table: '+@schema_name+' already exists')
        print(CHAR(13))
    END
END
GO

---------------------
-- Procedure to drop and recreate schema 
---------------------
CREATE PROCEDURE recreate_schema @schema_name SYSNAME
AS BEGIN
    EXEC drop_schema_cascade @schema_name
    EXEC create_schema @schema_name
END
GO

---------------------
-- Procedure to process creating external table query 
---------------------
CREATE PROCEDURE process_external_table @json NVARCHAR(max)
AS BEGIN
    DECLARE @table_creation_query VARCHAR(max), @schema_name VARCHAR(128), @table_name VARCHAR(128), @location VARCHAR(1024)

    SELECT @schema_name=schema_name, @table_name=table_name, @location=location, @table_creation_query=table_creation_query
    FROM OPENJSON(@json) 
    WITH (
        schema_name NVARCHAR(255) '$.schema_name',
        table_name NVARCHAR(255) '$.table_name',
        location NVARCHAR(512) '$.location',
        table_creation_query NVARCHAR(1024) '$.query'
    ) 
    print(@schema_name)
    print(CHAR(13))
    print(@table_name)
    print(CHAR(13))
    print(@location)
    print(CHAR(13))
    print(@table_creation_query)   

    EXEC create_schema @schema_name
    EXEC create_external_table @schema_name, @table_name, @table_creation_query

END
GO

----
DECLARE @json NVARCHAR(MAX);
SET @json = N'
{
  "schema_name": "weather",
  "table_name": "Bangalore",
  "location": "/weather-external/output/delta/weather/Bangalore",
  "query": "CREATE EXTERNAL TABLE weather.Bangalore ( \ntime varchar(4096) , \n tavg float , \n tmin float , \n tmax float , \n prcp float \n)\n WITH ( \n LOCATION =''/weather-external/output/delta/weather/Bangalore'', \n DATA_SOURCE = ADA_DS, \n FILE_FORMAT = DELTA_FORMAT_NAME \n ) \n"
} 
';

EXEC process_external_table @json

---
DROP PROCEDURE process_external_table