# pyscd
This is a pyspark package aimed at minimising code effort for implementing SCD type 2.

The package will he helpful incase one have large tables and high number of primary keys based on which one would want to implement a type 2 SCD.

One does not have to write logics for each table seperately. Just import this package and follow the usage given below and one should be good.



# Installation

```pip install pyscd```

# Example Usage


```from delta.tables import DeltaTable #import Deltatable
path_to_cleansed_layer = '/mnt/mycleansedlayer/data/CLEANSED_TABLE' #define the path to the cleansed layer of the table
df_raw = spark.read.table("raw_schema.RAW_TABLE") #read the raw table
df_cleansed = DeltaTable.forPath(spark, path_to_cleansed_layer) #read cleansed table as Delta table
primary_keys =['KEY1','KEY2',..]
----------------------------------------------------------------------------------------------------------------------
%sql
Refresh table cleansed_schema.CLEANSED_TABLE;
Refresh table raw_schema.RAW_TABLE;
VACUUM raw_schema.RAW_TABLE;
----------------------------------------------------------------------------------------------------------------------   
import pyscd as CDC
----------------------------------------------------------------------------------------------------------------------
CDC.update_del_records(df_raw,df_cleansed,primary_keys) #updates deleted records
----------------------------------------------------------------------------------------------------------------------
CDC.upsert_from_source(df_raw,df_cleansed,primary_keys) #updates inserted and updated records with merge