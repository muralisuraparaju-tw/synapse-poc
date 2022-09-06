#!/usr/bin/env python
# coding: utf-8

# ## csv2delta_pipeline_nb
#
#
#

# In[291]:


input_path_prefix = ""
file_name = ""
storage_account = ""


# In[292]:


if input_path_prefix == None or input_path_prefix == "":
    input_path_prefix = "weather-external/staging/weather"

if file_name == None or file_name == "":
    file_name = "Bangalore.csv"

if storage_account == None or storage_account == "":
    storage_account = "your-storage-account-name"


# In[293]:


'''
Constants
'''
tsql_data_type_dict = {}
tsql_data_type_dict['LongType']= 'bigint'
tsql_data_type_dict['BooleanType']= 'bit'
tsql_data_type_dict['DecimalType']= 'decimal'
tsql_data_type_dict['IntegerType']= 'int'
tsql_data_type_dict['ByteType']= 'smallint'
tsql_data_type_dict['ShortType']= 'smallint'
tsql_data_type_dict['DoubleType']= 'float'
tsql_data_type_dict['FloatType']= 'real'
tsql_data_type_dict['DateType']= 'date'
tsql_data_type_dict['TimestampType']= 'datetime2'
tsql_data_type_dict['char']= 'char'
tsql_data_type_dict['StringType']= 'varchar(4096)'
tsql_data_type_dict['BinaryType']= 'binary'

delta_data_type_dict = {}
delta_data_type_dict['LongType']= 'BIGINT'
delta_data_type_dict['BooleanType']= 'BOOLEAN'
delta_data_type_dict['DecimalType']= 'DECIMAL'
delta_data_type_dict['IntegerType']= 'INT'
delta_data_type_dict['ShortType']= 'SMALLINT'
delta_data_type_dict['DoubleType']= 'DOUBLE'
delta_data_type_dict['FloatType']= 'FLOAT'
delta_data_type_dict['DateType']= 'DATE'
delta_data_type_dict['TimestampType']= 'TIMESTAMP'
delta_data_type_dict['StringType']= 'STRING'
delta_data_type_dict['BinaryType']= 'BINARY'


lake_database_name = 'ada_lake'


# In[294]:


'''
Functions for file/path handling
'''
def get_dataset_and_container(path):
    elements = path.split("/")
    return (elements[-1], elements[0])

def get_file_prefix(file_name):
   elements = file_name.split(".")
   elements.pop()
   return "_".join(elements)

def get_interim_path(prefix, dataset):
    return prefix + "/" + "interim/" + dataset + "/"

def construct_abfss_path(storage_account, container_name, dataset_name, prefix):
    return ("abfss://" + container_name +"@" + storage_account+".dfs.core.windows.net/" + prefix +"/"+ dataset_name,
    "/"+ container_name + "/"+ prefix +"/"+ dataset_name)

def get_full_staging_file_name(input_path_prefix, file_name):
    dataset_name, container_name = get_dataset_and_container(input_path_prefix)
    abfss_path, relative_path = construct_abfss_path(storage_account, container_name, dataset_name, "staging")
    return abfss_path + "/" + file_name

def get_full_interim_file_name(input_path_prefix, file_name):
    dataset_name, container_name = get_dataset_and_container(input_path_prefix)
    abfss_path, relative_path = construct_abfss_path(storage_account, container_name, dataset_name, "output/delta")
    file_prefix = get_file_prefix(file_name)
    return (abfss_path + "/" + file_prefix,  dataset_name, file_prefix, relative_path)


# In[300]:


'''
Functions for formatting and saving the response (tsql schema)
'''
import json
def create_tsql_schema_sql(df, dataset_name, table_name, delta_location):
    table_sql = "CREATE EXTERNAL TABLE IF NOT EXISTS "+dataset_name+"."+table_name+" ( \n"
    schema = df.schema
    cols = []
    for entry in schema:
        col_name = entry.name
        spark_data_type = str(entry.dataType)
        tsql_data_type = tsql_data_type_dict[spark_data_type]
        col = col_name + " " + tsql_data_type
        cols.append(col)

    cols_sql = " , \n ".join(cols)
    table_sql += cols_sql +" \n)\n WITH ( \n"
    table_sql += " LOCATION = '"+delta_location+"', \n"
    table_sql += " DATA_SOURCE = SYNAPSE_DS \n"
    table_sql += " FILE_FORMAT = DELTA_FORMAT_NAME \n"
    table_sql += " ) \n"
    print('SQL Database table sql:'+table_sql)

    return table_sql

def create_result_json(df, dataset_name, file_prefix, relative_path):
    result_dict = {}
    location = relative_path + "/" + file_prefix
    tsql_schema_query = create_tsql_schema_sql(df, dataset_name, file_prefix, location)
    result_dict['schema_name'] = dataset_name
    result_dict['table_name'] = file_prefix
    result_dict['location'] = location
    result_dict['query'] = tsql_schema_query
    result_json = json.dumps(result_dict, indent=2)
    return result_json

def save_result_json(df, storage_account, file_prefix, input_path_prefix, relative_path):
    dataset_name, container_name = get_dataset_and_container(input_path_prefix)
    abfss_path, relative_path = construct_abfss_path(storage_account, container_name, dataset_name, "output/schema")
    result_json = create_result_json(df, dataset_name, file_prefix, relative_path)
    result_json_path = abfss_path +"/result.json"
    result_json_relative_path = relative_path + "/result.json"
    mssparkutils.fs.put(result_json_path, result_json, True)
    return result_json_relative_path


# In[301]:


'''
Functions for helping with delta table creation
'''
def create_database(db_name):
    db_sql = "CREATE DATABASE IF NOT EXISTS "+db_name
    print('Delta DB sql:'+db_sql)
    spark.sql(db_sql)

def create_delta_table(df, delta_location, dataset_name, table_name):
    print('Creating delta table:'+table_name+' in database:'+lake_database_name)
    table_sql = "CREATE TABLE IF NOT EXISTS "+dataset_name+"."+table_name+" ( \n"
    schema = df.schema
    cols = []
    for entry in schema:
        col_name = entry.name
        spark_data_type = str(entry.dataType)
        delta_data_type = delta_data_type_dict[spark_data_type]
        col = col_name + " " + delta_data_type
        cols.append(col)

    cols_sql = " , \n ".join(cols)
    table_sql += cols_sql +" \n)\n"
    table_sql += " USING DELTA \n"
    table_sql += " LOCATION '"+delta_location+"'\n"
    print('Delta table sql:'+table_sql)
    spark.sql(table_sql)

    return table_sql


# In[302]:


# Import modules - Create the delta table directly
from delta.tables import DeltaTable

def save_as_delta_file(df, delta_table_path, key_column):

    # If the delta table exists, merge (assume only one key_column for now)
    if (DeltaTable.isDeltaTable(spark, delta_table_path)):
        print('Merging to existing delta')
        # Read delta table
        delta_table = DeltaTable.forPath(spark, delta_table_path)

        # Merge new data into existing table
        delta_table.alias("existing").merge(
            source=df.alias("updates"),
            condition="existing." + key_column + " = updates." + key_column  # We look for matches on the name column
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()

    else:
        # Create new delta table with new data
        print('Creating new delta table')
        df.write.format('delta').save(delta_table_path)

def save_as_delta_table(df, delta_location, dataset_name, table_name):
    create_database(dataset_name)
    create_delta_table(df, delta_location, dataset_name, table_name)


# In[303]:


'''
The actual processing function
'''
def csv_2_delta():
    full_staging_file_path = get_full_staging_file_name(input_path_prefix, file_name)
    full_interim_file_path, dataset_name, file_prefix, relative_path = get_full_interim_file_name(input_path_prefix, file_name)


    df = spark.read.option("header","true").option("inferSchema", "true").csv(full_staging_file_path)
    print("Created dataframe for file:"+full_staging_file_path)

    save_as_delta_file(df, full_interim_file_path, 'time')
    print("Saved dataframe as delta: "+ full_interim_file_path)

    save_as_delta_table(df, full_interim_file_path, dataset_name, file_prefix)

    #result_json_path = save_result_json(df, storage_account, file_prefix, input_path_prefix, relative_path)
    result_json = create_result_json(df, dataset_name, file_prefix, relative_path)

    print("Result json path:"+str(result_json))
    return result_json


# In[304]:


print("input_path_prefix:"+input_path_prefix)
print("file_name:"+file_name)
print("storage_account:"+storage_account)

result_json = csv_2_delta()
mssparkutils.notebook.exit(result_json)


# In[261]:


get_ipython().run_cell_magic('sql', '', 'SELECT COUNT(*) FROM WEATHER.BANGALORE\n')
