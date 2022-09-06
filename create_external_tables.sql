-- Create database: One time task
CREATE DATABASE ADA

USE ADA;

-- One time (database level objects)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '???';

CREATE DATABASE SCOPED CREDENTIAL storage_account_managed_identity
WITH IDENTITY = 'Managed Identity'
GO

CREATE EXTERNAL DATA SOURCE ADA_DS WITH (
    LOCATION = 'https://your-storage-account.blob.core.windows.net',
    CREDENTIAL = storage_account_managed_identity
)
GO

CREATE EXTERNAL FILE FORMAT DELTA_FORMAT_NAME
WITH (
         FORMAT_TYPE = DELTA
      )
GO

-- Dataset specific objects
CREATE SCHEMA WEATHER
GO

-- -- Create the external table
CREATE EXTERNAL TABLE [WEATHER].[BANGALORE_WEATHER_EXTERNAL_TABLE]
(
    time VARCHAR(20),
    tavg FLOAT,
    tmin FLOAT,
    tmax FLOAT,
    prcp FLOAT
)
WITH (
    LOCATION = '/weather-external/output/Bangalore/',
    DATA_SOURCE = ADA_DS,
    FILE_FORMAT = DELTA_FORMAT_NAME
)
GO
--
CREATE EXTERNAL TABLE [WEATHER].[BANGALORE_WEATHER_EXTERNAL_TABLE_1]
(
    time VARCHAR(20),
    tavg FLOAT,
    tmin FLOAT,
    tmax FLOAT,
    prcp FLOAT
)
WITH (
    LOCATION = '/weather-external/output/Bangalore/',
    DATA_SOURCE = ADA_DS,
    FILE_FORMAT = DELTA_FORMAT_NAME
)
GO
--
CREATE EXTERNAL TABLE [WEATHER].[BANGALORE_WEATHER_EXTERNAL_TABLE_3]
(
    time VARCHAR(20),
    tavg FLOAT,
    tmin FLOAT,
    tmax FLOAT,
    prcp FLOAT
)
WITH ( LOCATION = '/weather-external/output/Bangalore/', DATA_SOURCE = ADA_DS, FILE_FORMAT = DELTA_FORMAT_NAME)
GO
--
CREATE EXTERNAL TABLE weather.Bangalore ( \ntime varchar(4096) , \n tavg float , \n tmin float , \n tmax float , \n prcp float \n)\n WITH ( \n LOCATION '/weather-external/output/delta/weather/Bangalore', \n DATA_SOURCE = ADA_DS \n FILE_FORMAT = DELTA_FORMAT_NAME \n )

SELECT COUNT(*) FROM [WEATHER].[BANGALORE_WEATHER_EXTERNAL_TABLE]
SELECT * FROM [WEATHER].[BANGALORE_WEATHER_EXTERNAL_TABLE]

DROP EXTERNAL TABLE WEATHER.BANGALORE_WEATHER_EXTERNAL_TABLE_2
DROP SCHEMA [WEATHER]

SELECT COUNT(*) FROM sys.external_tables WHERE name = [WEATHER].[BANGALORE_WEATHER_EXTERNAL_TABLE]

SELECT * FROM sys.external_tables
SELECT * FROM sys.procedures
SELECT * FROM sys.schemas

SELECT a.name from sys.external_tables a, sys.schemas b WHERE a.schema_id=b.schema_id AND b.name='WEATHER'

DROP TABLE a.name from sys.external_tables a, sys.schemas b WHERE a.schema_id=b.schema_id AND b.name='WEATHER'

DROP PROCEDURE drop_schema_if_exists



select
    *
from openrowset(
        bulk 'weather-external/output/schema/weather/result.json',
        data_source = 'ADA_DS',
        format = 'csv',
        fieldterminator ='0x0b',
        fieldquote = '0x0b'
    ) with (doc nvarchar(max)) as rows

----------------------------
----------------------------
DECLARE @json NVARCHAR(MAX);
SET @json = N'[
  {"id": 2, "info": {"name": "John", "surname": "Smith"}, "age": 25},
  {"id": 5, "info": {"name": "Jane", "surname": "Smith", "skills": ["SQL", "C#", "Azure"]}, "dob": "2005-11-04T12:00:00"}
]';

SELECT id, firstName, lastName, age, dateOfBirth, skill
FROM OPENJSON(@json)
  WITH (
    id INT 'strict $.id',
    firstName NVARCHAR(50) '$.info.name',
    lastName NVARCHAR(50) '$.info.surname',
    age INT,
    dateOfBirth DATETIME2 '$.dob',
    skills NVARCHAR(MAX) '$.info.skills' AS JSON
  )
OUTER APPLY OPENJSON(skills)
  WITH (skill NVARCHAR(8) '$');

----------------------------
----------------------------
DECLARE @json NVARCHAR(MAX);
SET @json = N'
{
  "schema_name": "weather",
  "table_name": "Bangalore",
  "location": "/weather-external/output/delta/weather/Bangalore",
  "query": "CREATE EXTERNAL TABLE weather.Bangalore ( \ntime varchar(4096) , \n tavg float , \n tmin float , \n tmax float , \n prcp float \n) WITH (LOCATION=''/weather-external/output/delta/weather/Bangalore'', DATA_SOURCE = ADA_DS, FILE_FORMAT = DELTA_FORMAT_NAME )"
}
';

DECLARE @query NVARCHAR(max), @schema_name VARCHAR(200), @table_name VARCHAR(200), @location VARCHAR(1024)
--CREATE TABLE #exec_temp(query VARCHAR(max), location VARCHAR(max) )

--INSERT INTO #exec_temp(query, location)
SELECT @schema_name=schema_name, @table_name=table_name, @location=location, @query=query
FROM OPENJSON(@json)
WITH (
    schema_name NVARCHAR(50) '$.schema_name',
    table_name NVARCHAR(50) '$.table_name',
    location NVARCHAR(100) '$.location',
    query NVARCHAR(max) '$.query'
)
print(@schema_name)
print(CHAR(13))
print(@table_name)
print(CHAR(13))
print(@location)
print(CHAR(13))
print(@query)
EXEC sp_executesql @query
----------------------------
----------------------------

SELECT COUNT(a.name) from sys.external_tables a, sys.schemas b WHERE a.schema_id=b.schema_id AND b.name= 'weather' AND a.name = 'Bangalore'

SELECT GETDATE()
