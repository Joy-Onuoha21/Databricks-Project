# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dev3
# MAGIC MANAGED LOCATION 'abfss://unity-catalog-storage@dbstoragedlwvnba4d5k72.dfs.core.windows.net/2141168493853526';
# MAGIC
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS dev3.demodb;
# MAGIC
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS dev3.demodb.landing_zone
# MAGIC LOCATION 'abfss://dbfs-container@joyonuoha.dfs.core.windows.net/archive'

# COMMAND ----------

geolocation = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true") \
    .load("/Volumes/dev3/demodb/landing_zone/olist_geolocation_dataset.csv")

# COMMAND ----------

geolocation.write.format("parquet")\
    .mode("overwrite") \
    .save("/Volumes/dev3/demodb/parquet/olist_geolocation_dataset")

# COMMAND ----------

from delta.tables import DeltaTable

DeltaTable.convertToDelta(
    spark,
    "parquet.`/Volumes/dev3/demodb/parquet/olist_geolocation_dataset`"
)

# COMMAND ----------

geolocation.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("dev3.demodb.olist_geolocation_dataset")

# COMMAND ----------

import pyspark.sql.functions as f

geolocation_dt = DeltaTable.forName(spark, "dev3.demodb.olist_geolocation_dataset")

geolocation_dt.update(
  condition = "geolocation_zip_code_prefix = '1037'",
  set = { "geolocation_city": f.initcap("geolocation_state") }
)

# COMMAND ----------

geolocation_dt.delete("geolocation_state = 'SP'")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history dev3.demodb.olist_geolocation_dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table dev3.demodb.olist_geolocation_dataset to timestamp as of '2025-05-02T16:22:54.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED dev3.demodb.olist_geolocation_dataset

# COMMAND ----------

geolocation = spark.read.table("dev3.demodb.olist_geolocation_dataset")


# COMMAND ----------

import os

# Define base paths
landing_zone_path = "/Volumes/dev/demodb/landing_zone"
bronze_path = "/Volumes/dev3/demodb/bronze"

# List all CSV files in the landing zone
files = [f.name for f in dbutils.fs.ls(landing_zone_path) if f.name.endswith(".csv")]

for file in files:
    name = file.replace(".csv", "")
    input_path = os.path.join(landing_zone_path, file)
    output_path = os.path.join(bronze_path, name)

    # Read CSV
    df = (spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(input_path)
    )

    # Write as Delta to bronze layer
    (df.write
       .format("delta")
       .mode("overwrite")
       .save(output_path)
    )

    print(f"✔️ Converted {file} to Delta at {output_path}")
