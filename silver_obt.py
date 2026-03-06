# Databricks notebook source
# MAGIC %md
# MAGIC Stream Rides Transformation
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

rides_schema =StructType([StructField('ride_id', StringType(), True), StructField('confirmation_number', StringType(), True), StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', LongType(), True), StructField('vehicle_make_id', LongType(), True), StructField('payment_method_id', LongType(), True), StructField('ride_status_id', LongType(), True), StructField('pickup_city_id', LongType(), True), StructField('dropoff_city_id', LongType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', LongType(), True), StructField('booking_timestamp', TimestampType(), True), StructField('pickup_timestamp', StringType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)])

# COMMAND ----------

df = spark.read.table("uber.bronze.rides_raw")
df_parsed = df.withColumn("parsed_rides",from_json(col("rides"),rides_schema)).select("parsed_rides.*")
display(df_parsed)

# COMMAND ----------

df = spark.sql("select * from uber.bronze.bulk_rides")
rides_schema=df.schema

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.stg_rides

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     passenger_name,
# MAGIC     passenger_email,
# MAGIC     passenger_id,
# MAGIC     passenger_phone
# MAGIC FROM
# MAGIC     uber.bronze.silver_obt

# COMMAND ----------

df = spark.read.table("uber.bronze.silver_obt")

df = df.select(
    "distance_miles",
    "duration_minutes",
    "base_fare",
    "distance_fare",
    "time_fare",
    "surge_multiplier",
    "total_fare",
    "tip_amount",
    "rating",
    "base_rate",
    "per_mile",
    "per_minute"
)

df = df.dropDuplicates(subset=["ride_id"])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     fact.ride_id,
# MAGIC     fact.base_fare,
# MAGIC     dim.region
# MAGIC FROM uber.bronze.fact AS fact
# MAGIC LEFT JOIN uber.bronze.dim_location AS dim
# MAGIC ON fact.pickup_city_id = dim.pickup_city_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Jinja Template for OBT

# COMMAND ----------

pip install jinja2

# COMMAND ----------

jinja_config = [
    {
        "table": "uber.bronze.stg_rides stg_rides",
        "select": "stg_rides.*",
        "where": ""
    },
    {
        "table": "uber.bronze.map_vehicle_makes map_vehicle_makes",
        "select": "map_vehicle_makes.*",
        "where": "",
        "on": "stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id"
    },
    {
        "table": "uber.bronze.map_vehicle_types map_vehicle_types",
        "select": "map_vehicle_types.vehicle_type, map_vehicle_types.description, map_vehicle_types.base_rate, map_vehicle_types.per_mile, map_vehicle_types.per_minute",
        "where": "",
        "on": "stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id"
    }
]

# COMMAND ----------

from jinja2 import Template

jinja_str = """

SELECT
{% for config in jinja_config %}
    {{ config.select }}
    {% if not loop.last %}
        ,
    {% endif %}
{% endfor %}

FROM
{% for config in jinja_config %}
    {% if loop.first %}
        {{ config.table }}
    {% else %}
        LEFT JOIN {{ config.table }} ON {{ config.on }}
    {% endif %}
{% endfor %}

{% set where_clauses = [] %}
{% for config in jinja_config %}
    {% if config.where != "" %}
        {% set _ = where_clauses.append(config.where) %}
    {% endif %}
{% endfor %}

{% if where_clauses | length > 0 %}
WHERE {{ where_clauses | join(" AND ") }}
{% endif %}

"""

template = Template(jinja_str)
render_template = template.render(jinja_config=jinja_config)

print(render_template)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC    stg_rides.*
# MAGIC FROM
# MAGIC    uber.bronze.stg_rides stg_rides
# MAGIC LEFT JOIN
# MAGIC    uber.bronze.map_vehicle_types map_vehicle_types
# MAGIC ON
# MAGIC    stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id
# MAGIC LEFT JOIN
# MAGIC    uber.bronze.map_vehicle_makes map_vehicle_makes
# MAGIC ON
# MAGIC    stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id

# COMMAND ----------

spark.sql(render_template)

# COMMAND ----------

spark.sql(render_template).show()

# COMMAND ----------

display(spark.sql(render_template))

# COMMAND ----------

template = Template(jinja_str)
rendered_template = template.render(jinja_config=jinja_config)
display(spark.sql(rendered_template))

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp()