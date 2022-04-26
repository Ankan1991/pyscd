import pyspark.sql.functions as F

def handle_special_chars_in_cols(columns,chars):
  """
  columns : list of columns you wnat to handle. Ideally this should be all the dataframe columns
  chars : list of special characters you want to handle
  """
  return [f"`{i}`" if any((char in i for char in chars )) else f"{i}" for i in columns]
  
  

def update_del_records(raw, cleansed, keys ,columnspecialchars = ['$'],hashkeyColumn ='objversion' ):
  """
  raw: Raw Dataframe (Spark DataFrame)
  cleansed : Cleansed Delta Table (Not spark DataFrame)
  keys : Primary Keys to join
  columnspecialchars : list of special characters in columns you want to handle . Default value is ['$']
  hashkeyColumn: Name of the hash column used as a reference for merge
  
  """
  cleansed_df = cleansed.toDF()
  keys = handle_special_chars_in_cols(keys ,columnspecialchars) 
  where_clause = ' and '.join([f"r.{i} is null" for i in keys]) + " and c.ACTIVE_IND == 'Y'"

  joined = (cleansed_df.select(*keys+['ACTIVE_IND']).alias("c").join(raw.alias("r"),how='left_outer',
                           on= [F.col(f"c.{a}")==F.col(f"r.{b}") for a,b in zip(keys,keys)])
                       .where(where_clause).select("c.*").drop('ACTIVE_IND'))
  
  update = {'CDC_FLAG':F.lit('D'), 
          'CDC_END_TS' : F.to_timestamp(F.current_timestamp(), 'yyyy-MM-dd HH:mm:ss'),
         'ACTIVE_IND' : F.lit('N')}
  merge_condition = " and ".join([f"deleted.{i} = cleansed.{i}" for i in keys])
  
  (cleansed.alias("cleansed").merge(joined.alias("deleted"),
                                    merge_condition)
             .whenMatchedUpdate(set=update).execute())
             

def upsert_from_source(raw,cleansed,keys,columnspecialchars = ['$'],hashkeyColumn ='objversion'):
  """
  raw: Raw Dataframe (Spark DataFrame)
  cleansed : Cleansed Delta Table (Not spark DataFrame)
  keys : Primary Keys to join
  columnspecialchars : list of special characters in columns you want to handle . Default value is ['$']
  hashkeyColumn: Name of the hash column used as a reference for merge

  """
  cleansed_df = cleansed.toDF()
  keys = handle_special_chars_in_cols(keys ,columnspecialchars)
  merge_keys = [f"mergekey{i}" for i in range(1,len(keys)+1)]
  staged_updates = raw.select("*",*[F.col(b).alias(a) for a,b in zip(merge_keys,keys)]).unionAll(
  raw.select("*",*[F.lit(None).alias(a) for a in merge_keys]).alias("raw").join(
  cleansed_df.alias("cleansed"),
  on = [F.col(f"raw.{a}")==F.col(f"cleansed.{a}") for a in keys], how='inner').select("raw.*")
  .where(f"cleansed.active_ind == 'Y' AND raw.{hashkeyColumn} <> cleansed.{hashkeyColumn} ")
  )
  cleansed_columns = cleansed_df.columns
  #list the hardcoded columns (with alias here) which is to be inserted when merging
  flag_columns = ["cleansed.CDC_FLAG", "cleansed.CDC_START_TS" ,"cleansed.CDC_END_TS","cleansed.ACTIVE_IND"]
  #list the hardcoded values which is to be inserted when merging
  insert_values = [F.lit('I'),F.to_timestamp(F.current_timestamp(),'yyyy-MM-dd HH:mm:ss'),
                   F.lit("9999-12-01 00:00:00"),F.lit('Y')]
  #create a dynamic dictionary for inserting which consists of the hardcoded columns
  handle_special_cols = handle_special_chars_in_cols(cleansed_columns ,columnspecialchars)  
  inserts = {f"cleansed.{i}":f"staged.{i}" for i in handle_special_cols if i not in flag_columns}
  #now insert the other columns which is to be inserted from the staging updates dataframe
  inserts.update(dict(zip(flag_columns,insert_values)))
  #create the updates dictionary for flagging the updates
  updates ={"cleansed.active_ind": F.lit('N'), "cleansed.cdc_flag": F.lit('U'), 
          "cleansed.cdc_END_ts": F.to_timestamp(F.current_timestamp(), 'yyyy-MM-dd HH:mm:ss')}
  upsert_merge_condition = " and ".join([f"cleansed.{a} = staged.{b}" 
                                    for a,b in zip(keys,
                                    [f"mergekey{i}" for i in range(1,len(keys)+1)])])
  (cleansed.alias("cleansed").merge(staged_updates.alias("staged"),
                upsert_merge_condition)
                .whenMatchedUpdate(condition = f"cleansed.active_ind = 'Y' AND cleansed.{hashkeyColumn} <> staged.{hashkeyColumn} ",                                    set=updates)
                .whenNotMatchedInsert(values = inserts)
                .execute())