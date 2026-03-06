
# Uber Real-Time Data Engineering Pipeline 🚀

This project demonstrates an **end-to-end real-time data engineering pipeline** built using **Azure Databricks, Delta Lake, and PySpark** following the **Lakehouse architecture**.

The pipeline processes Uber ride events, builds streaming transformations, and creates a **star schema data model** for analytics.

---

## Architecture

Raw Events → Bronze → Staging → Silver OBT → Dimension Tables → Fact Table

---

## Tech Stack

- Azure Databricks
- Delta Lake
- PySpark
- Azure Data Lake Storage
- Spark SQL
- Databricks Lakeflow Pipelines
- Event Hubs (Kafka interface)

---

## Pipeline Flow

### 1️⃣ Data Ingestion
Ride events are ingested as raw data into the **Bronze layer**.

### 2️⃣ Staging Layer
Streaming transformations prepare the data in `stg_rides`.

### 3️⃣ Silver Layer
An **Operational Business Table (OBT)** is created by joining mapping tables.

### 4️⃣ Dimensional Modeling
Dimension tables created:

- dim_driver
- dim_passenger
- dim_vehicle
- dim_location
- dim_payment
- dim_booking

### 5️⃣ Fact Table

The fact table contains ride metrics:

- base_fare
- distance_fare
- duration_minutes
- surge_multiplier
- total_fare
- tip_amount
- rating

---

## Example Query

```sql
SELECT fact.ride_id, fact.base_fare, dim.region
FROM uber.bronze.fact AS fact
LEFT JOIN uber.bronze.dim_location AS dim
ON fact.pickup_city_id = dim.pickup_city_id;
```

---

## Pipeline Graph

(Add your Databricks pipeline screenshot here)

---

## Learning Outcomes

- Building real-time streaming pipelines
- Implementing Slowly Changing Dimensions
- Designing a Lakehouse architecture
- Using Delta streaming tables
- End-to-end data engineering workflows

---

## Acknowledgement

Special thanks to **Ansh Lamba** for the learning resources and guidance for this project.
