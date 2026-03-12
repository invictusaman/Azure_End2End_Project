# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys 

project_path = os.path.join((os.getcwd()),"..","..")

sys.path.append(project_path)

from utils.transformations import ReusableClass

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## DimUser

# COMMAND ----------

df_static = spark.read.format("parquet") \
    .load("abfss://bronze-end2end@storageend2end.dfs.core.windows.net/DimUser")

display(df_static)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoloader - Spark Structured Streaming

# COMMAND ----------

df_user = spark.readStream.format('cloudFiles')\
                .option("cloudFiles.format","parquet")\
                .option("cloudFiles.schemaLocation", "abfss://silver-end2end@storageend2end.dfs.core.windows.net/DimUser/read_checkpoints")\
                .load("abfss://bronze-end2end@storageend2end.dfs.core.windows.net/DimUser")    

# COMMAND ----------

# display(df_user, checkpointLocation = "abfss://silver-end2end@storageend2end.dfs.core.windows.net/DimUser/checkpoints")

# (
#     df_user.writeStream
#         .format("memory")
#         .queryName("debug_table")
#         .outputMode("append")
#         .trigger(once=True)
#         .option("checkpointLocation", "abfss://silver-end2end@storageend2end.dfs.core.windows.net/DimUser/debug_checkpoint")
#         .start()
# )

# spark.sql("SELECT * FROM debug_table").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Some Transformations

# COMMAND ----------

df_user = df_user.withColumn("user_name",upper(col("user_name")))

df_user_obj = ReusableClass()
df_user = df_user_obj.dropColumns(df_user, '_rescued_data')
df_user = df_user.dropDuplicates() 

# COMMAND ----------

df_user.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver-end2end@storageend2end.dfs.core.windows.net/DimUser/write_checkpoints")\
        .trigger(once=True)\
        .toTable("end2endCata.silver.DimUser")
        #.option("path","abfss://silver-end2end@storageend2end.dfs.core.windows.net/DimUser/data")\
        

# COMMAND ----------

# MAGIC %md
# MAGIC ## DimArtist

# COMMAND ----------

df_static = spark.read.format("parquet") \
    .load("abfss://bronze-end2end@storageend2end.dfs.core.windows.net/DimArtist")

display(df_static)

# COMMAND ----------

df_artist = spark.readStream.format('cloudFiles')\
                .option("cloudFiles.format","parquet")\
                .option("cloudFiles.schemaLocation", "abfss://silver-end2end@storageend2end.dfs.core.windows.net/DimArtist/read_checkpoints")\
                .load("abfss://bronze-end2end@storageend2end.dfs.core.windows.net/DimArtist")    

# COMMAND ----------

df_artist = df_artist.withColumn("artist_name",upper(col("artist_name")))
df_artist_obj = ReusableClass()
df_artist = df_artist_obj.dropColumns(df_artist, '_rescued_data')
df_artist = df_artist.dropDuplicates()

# COMMAND ----------

df_artist.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver-end2end@storageend2end.dfs.core.windows.net/DimArtist/write_checkpoints")\
        .trigger(once=True)\
        .toTable("end2endCata.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DimTrack

# COMMAND ----------

df_static = spark.read.format("parquet") \
    .load("abfss://bronze-end2end@storageend2end.dfs.core.windows.net/DimTrack")

display(df_static)

# COMMAND ----------

df_track = spark.readStream.format('cloudFiles')\
                .option("cloudFiles.format","parquet")\
                .option("cloudFiles.schemaLocation", "abfss://silver-end2end@storageend2end.dfs.core.windows.net/DimTrack/read_checkpoints")\
                .load("abfss://bronze-end2end@storageend2end.dfs.core.windows.net/DimTrack")    

# COMMAND ----------

df_track = (
    df_track
    .withColumn("track_name", regexp_replace(col("track_name"), "-", " "))
    .withColumn("album_name", upper(col("album_name")))
    .withColumn(
        "durationFlag",
        when(col("duration_sec") >= 300, "Long")
        .when(col("duration_sec") >= 120, "Medium")
        .otherwise("Short")
    )
)

df_track = ReusableClass().dropColumns(df_track, ["_rescued_data"]).dropDuplicates()

# COMMAND ----------

df_track.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver-end2end@storageend2end.dfs.core.windows.net/DimTrack/write_checkpoints")\
        .trigger(once=True)\
        .toTable("end2endCata.silver.DimTrack")

# COMMAND ----------

spark.sql('''
          select * from end2endCata.silver.DimTrack''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DimDate

# COMMAND ----------

df_static = spark.read.format("parquet") \
    .load("abfss://bronze-end2end@storageend2end.dfs.core.windows.net/DimDate")
display(df_static)          

# COMMAND ----------

df_date = spark.readStream.format('cloudFiles')\
                .option("cloudFiles.format","parquet")\
                .option("cloudFiles.schemaLocation", "abfss://silver-end2end@storageend2end.dfs.core.windows.net/DimDate/read_checkpoints")\
                .load("abfss://bronze-end2end@storageend2end.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date = ReusableClass().dropColumns(df_date, ["_rescued_data"]).dropDuplicates()
df_date.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver-end2end@storageend2end.dfs.core.windows.net/DimDate/write_checkpoints")\
        .trigger(once=True)\
        .toTable("end2endCata.silver.DimDate")

# COMMAND ----------

spark.sql('''
          select count(*) from end2endCata.silver.DimDate''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## FactStream

# COMMAND ----------

df_static = spark.read.format('parquet')\
                .load("abfss://bronze-end2end@storageend2end.dfs.core.windows.net/FactStream")
display(df_static)

# COMMAND ----------

df_fact = spark.readStream.format('cloudFiles')\
                .option("cloudFiles.format","parquet")\
                .option("cloudFiles.schemaLocation", "abfss://silver-end2end@storageend2end.dfs.core.windows.net/FactStream/read_checkpoints")\
                .load("abfss://bronze-end2end@storageend2end.dfs.core.windows.net/FactStream")

# COMMAND ----------

# df_track_table = spark.table("end2endCata.silver.DimTrack")
# df_fact_enriched = df_fact.join(
#                     df_track_table.select("track_id", "durationFlag"),
#                     on="track_id",
#                     how="left"
# )

# COMMAND ----------

df_fact = ReusableClass().dropColumns(df_fact, ["_rescued_data"]).dropDuplicates()

# COMMAND ----------

df_fact.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver-end2end@storageend2end.dfs.core.windows.net/FactStream/write_checkpoints")\
        .trigger(once=True)\
        .toTable("end2endCata.silver.FactStream")