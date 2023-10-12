# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Streaming homework
# MAGIC
# MAGIC ## TODO:
# MAGIC * Create Spark Structured Streaming application with Auto Loader to incrementally and efficiently processes hotel/weather data as it arrives in provisioned Storage Account. Using Spark calculate in Databricks Notebooks for each city each day:
# MAGIC   * Number of distinct hotels in the city.
# MAGIC   * Average/max/min temperature in the city.
# MAGIC * Visualize incoming data in Databricks Notebook for 10 biggest cities (the biggest number of hotels in the city, one chart for one city):
# MAGIC   * X-axis: date (date of observation).
# MAGIC   * Y-axis: number of distinct hotels, average/max/min temperature.
# MAGIC * Deploy Databricks Notebook on cluster, to setup infrastructure use terraform scripts from module. Default resource parameters (specifically memory) will not work because of free tier limitations. You needed to setup memory and cores properly.
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------



hw_stream = spark.readStream \
    .format("cloudFiles") \
    .schema("address string, avg_tmpr_c double, avg_tmpr_f double, city string, country string, geoHash string, id string, latitude double, longitude double, name string, wthr_date string, wthr_year string, wthr_month string, wthr_day string") \
    .option("cloudFiles.format", "parquet") \
    .load("gs://storage-bucket-select-gar/m13sparkstreaming/hotel-weather")


# COMMAND ----------

hw_stream \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoints/") \
    .toTable("hotel_weather") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as total_records_number from hotel_weather

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate in for each city each day:
# MAGIC * Number of distinct hotels in the city.

# COMMAND ----------

# MAGIC %sql
# MAGIC select wthr_date, city, count(*) as count_by_city
# MAGIC from hotel_weather
# MAGIC group by wthr_date, city
# MAGIC order by count_by_city desc, wthr_date

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate in for each city each day:
# MAGIC * Average/max/min temperature in the city.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct avg_tmpr_c from hotel_weather where city='Albuquerque'

# COMMAND ----------

# MAGIC %sql
# MAGIC select wthr_date, city, 
# MAGIC   round(min(avg_tmpr_c), 2) as min_temperature_c,
# MAGIC   round(avg(avg_tmpr_c), 2) as mean_temperature_c,
# MAGIC   round(max(avg_tmpr_c), 2) as max_temperature_c,
# MAGIC   count(*) as records
# MAGIC from hotel_weather
# MAGIC group by wthr_date, city
