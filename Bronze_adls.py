# Databricks notebook source
import pandas as pd

sas = "sp=r&st=2026-03-05T18:54:03Z&se=2026-03-30T03:09:03Z&spr=https&sv=2024-11-04&sr=c&sig=SMIqZTl8SMYSicXQraN5fvRXnoDsnWBRTg3y3jRtd1w%3D"

df = pd.read_json(
    f"https://datalakezuberdevz.blob.core.windows.net/raw/ingestion/map_cities.json.json?{sas}"
)

df_spark = spark.createDataFrame(df)

display(df_spark)

# COMMAND ----------

import pandas as pd

sas = "sp=r&st=2026-03-05T18:54:03Z&se=2026-03-30T03:09:03Z&spr=https&sv=2024-11-04&sr=c&sig=SMIqZTl8SMYSicXQraN5fvRXnoDsnWBRTg3y3jRtd1w%3D"

files = [
"map_cities",
"map_cancellation_reasons",
"bulk_rides",
"map_payment_methods",
"map_ride_statuses",
"map_vehicle_makes",
"map_vehicle_types"
]

for file in files:

    url = f"https://datalakezuberdevz.blob.core.windows.net/raw/ingestion/{file}.json.json?{sas}"

    df = pd.read_json(url)

    df_spark = spark.createDataFrame(df)

    df_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema","true") \
        .saveAsTable(f"uber.bronze.{file}")

    print(f"{file} loaded successfully")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.map_cities

# COMMAND ----------

# MAGIC
# MAGIC %sql 
# MAGIC SELECT * FROM uber.bronze.rides_raw