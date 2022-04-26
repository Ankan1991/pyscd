# pyscd
This is a pyspark package aimed at minimising code effort for implementing SCD type 2.

The package will he helpful incase one have large tables and high number of primary keys based on which one would want to implement a type 2 SCD.

One does not have to write logics for each table seperately. Just import this package and follow the usage given below and one should be good.

**Note that your DDL statement for the cleansed table should have 4 audit columns shown below**


 ```
 ,CDC_FLAG                           STRING -- This Flag indicates whether the current row is a New Insert ("I") or an update ("U") or deleted ("D")
 
,CDC_START_TS                        TIMESTAMP

,CDC_END_TS                          TIMESTAMP

,ACTIVE_IND                          STRING -- This Flag determines the latest value. If the value is "Y" , consider it as the latest record else "N"
```




# Installation

```pip install pyscd```

# Example Usage


```
from delta.tables import DeltaTable #import Deltatable
path_to_cleansed_layer = '/mnt/mycleansedlayer/data/CLEANSED_TABLE' #define the path to the cleansed layer of the table

df_raw = spark.read.table("raw_schema.RAW_TABLE") #read the raw table
df_cleansed = DeltaTable.forPath(spark, path_to_cleansed_layer) #read cleansed table as Delta table
primary_keys =['KEY1','KEY2',..]

----------------------------------------------------------------------------------------------------------------------   
import pyscd as CDC
----------------------------------------------------------------------------------------------------------------------
CDC.update_del_records(df_raw,df_cleansed,primary_keys) #updates deleted records
----------------------------------------------------------------------------------------------------------------------
CDC.upsert_from_source(df_raw,df_cleansed,primary_keys) #updates inserted and updated records with merge
