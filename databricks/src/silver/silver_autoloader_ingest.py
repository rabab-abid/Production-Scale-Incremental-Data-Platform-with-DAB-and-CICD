# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *


# COMMAND ----------

import os,sys
sys.path.append(os.path.join(os.getcwd(), '..', '..'))

from utils.transformations import reusable

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
df_user = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation",
        "abfss://silver@musicmetadataadls.dfs.core.windows.net/DimUser/checkpoint")
    .load("abfss://bronze@musicmetadataadls.dfs.core.windows.net/DimUser/"))
df_user = df_user.withColumn("user_name", F.upper(F.col("user_name")))

df_user_obj=reusable()
df_user=df_user_obj.dropColumns(df_user,['_rescued_data'])


df_user.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation",
        "abfss://silver@musicmetadataadls.dfs.core.windows.net/DimUser/checkpoint") \
    .trigger(once=True) \
    .option("path",
        "abfss://silver@musicmetadataadls.dfs.core.windows.net/DimUser/data") \
    .toTable("musicmeta_dev.silver.DimUser")

# COMMAND ----------

import os,sys
sys.path.append(os.path.join(os.getcwd(), '..', '..'))

from utils.transformations import reusable

from pyspark.sql import functions as F
from pyspark.sql.types import *
df_track = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation",
        "abfss://silver@musicmetadataadls.dfs.core.windows.net/DimTrack/checkpoint")
    .load("abfss://bronze@musicmetadataadls.dfs.core.windows.net/DimTrack/"))
df_track = df_track.withColumn(
    "durationflag",
    F.when(F.col("duration_sec") < 150, "Low")
     .when(F.col("duration_sec") < 300, "Medium")
     .otherwise("high")
)
df_track=df_track.withColumn("track_name", F.regexp_replace(F.col('track_name'),'-',' '))

df_track_obj=reusable()                             
df_track=df_track_obj.dropColumns(df_track,['_rescued_data'])


df_track.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation",
        "abfss://silver@musicmetadataadls.dfs.core.windows.net/DimTrack/checkpoint") \
    .trigger(once=True) \
    .option("path",
        "abfss://silver@musicmetadataadls.dfs.core.windows.net/DimTrack/data") \
    .toTable("musicmeta_dev.silver.DimTrack")

# COMMAND ----------

import os,sys
sys.path.append(os.path.join(os.getcwd(), '..', '..'))

from utils.transformations import reusable

from pyspark.sql import functions as F
from pyspark.sql.types import *
df_date = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation",
        "abfss://silver@musicmetadataadls.dfs.core.windows.net/DimDate/checkpoint")
    .load("abfss://bronze@musicmetadataadls.dfs.core.windows.net/DimDate/"))

df_date_obj=reusable()                             
df_date=df_date_obj.dropColumns(df_date,['_rescued_data'])


df_date.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation",
        "abfss://silver@musicmetadataadls.dfs.core.windows.net/DimDate/checkpoint") \
    .trigger(once=True) \
    .option("path",
        "abfss://silver@musicmetadataadls.dfs.core.windows.net/DimDate/data") \
    .toTable("musicmeta_dev.silver.DimDate")

# COMMAND ----------

import os,sys
sys.path.append(os.path.join(os.getcwd(), '..', '..'))

from utils.transformations import reusable

from pyspark.sql import functions as F
from pyspark.sql.types import *
df_fact = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation",
        "abfss://silver@musicmetadataadls.dfs.core.windows.net/FactStream/checkpoint")
    .load("abfss://bronze@musicmetadataadls.dfs.core.windows.net/FactStream/"))

df_fact_obj=reusable()                             
df_fact=df_fact_obj.dropColumns(df_fact,['_rescued_data'])

df_fact.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation",
        "abfss://silver@musicmetadataadls.dfs.core.windows.net/FactStream/checkpoint") \
    .trigger(once=True) \
    .option("path",
        "abfss://silver@musicmetadataadls.dfs.core.windows.net/FactStream/data") \
    .toTable("musicmeta_dev.silver.factstream")