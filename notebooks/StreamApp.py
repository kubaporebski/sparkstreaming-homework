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

# remove old files, if they are there
dbutils.fs.rm("dbfs:/tmp/checkpoints", True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/hotel_weather", True)

# for limited time of streaming
import time 
import threading

def wait_and_stop(query, time_limit):
    print(f"Running for {time_limit} seconds... ")
    time.sleep(time_limit)
    query.stop()
    print("Done. ")

# COMMAND ----------

# create a stream
hw_stream = spark.readStream \
    .format("cloudFiles") \
    .schema("address string, avg_tmpr_c double, avg_tmpr_f double, city string, country string, geoHash string, id string, latitude double, longitude double, name string, wthr_date string, wthr_year string, wthr_month string, wthr_day string") \
    .option("cloudFiles.format", "parquet") \
    .load("gs://storage-bucket-select-gar/m13sparkstreaming/hotel-weather")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data description
# MAGIC
# MAGIC Weather-Hotels data joined by 4-characters geohash.
# MAGIC
# MAGIC | Column name | Description | Data type | Partition |
# MAGIC | --- | --- | --- | --- |
# MAGIC | address | Hotel address | string | no |
# MAGIC | avg_tmpr_c | Average temperature in Celsius | double | no |
# MAGIC | avg_tmpr_f | Average temperature in Fahrenheit | double | no |
# MAGIC | city | Hotel city | string | no |
# MAGIC | country | Hotel country | string | no |
# MAGIC | geoHash | 4-characters geohash based on Longitude & Latitude | string | no |
# MAGIC | id | ID of hotel | string | no |
# MAGIC | latitude | Latitude of a weather station | double | no |
# MAGIC | longitude | Longitude of a weather station | double | no |
# MAGIC | name | Hotel name | string | no |
# MAGIC | wthr_date | Date of observation (YYYY-MM-DD) | string | no |
# MAGIC | wthr_year | Year of observation (YYYY) | string | yes |
# MAGIC | wthr_month | Month of observation (MM) | string | yes |
# MAGIC | wthr_day | Day of observation (DD) | string | yes |
# MAGIC

# COMMAND ----------

# DBTITLE 1,Start a streaming
# how much time we will be running this (current cell) streaming code? [seconds]
time_limit = 120 

# run the stream
query = hw_stream \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoints/") \
    .toTable("hotel_weather") 

thr = threading.Thread(target=wait_and_stop, args=(query, time_limit), daemon=True)
thr.start()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check how many records are there now
# MAGIC select concat('There are ', count(*), ' rows currently.') as total_records_number_info from hotel_weather

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate in for each city each day:
# MAGIC * Number of distinct hotels in the city.

# COMMAND ----------

# MAGIC %sql
# MAGIC select wthr_date, city, count(distinct id) as distinct_hotels_by_city
# MAGIC from hotel_weather
# MAGIC group by city, wthr_date
# MAGIC order by wthr_date, distinct_hotels_by_city desc

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate in for each city each day:
# MAGIC * Average/max/min temperature in the city.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select wthr_date, city, 
# MAGIC   round(min(avg_tmpr_c), 2) as min_temperature_c,
# MAGIC   round(avg(avg_tmpr_c), 2) as mean_temperature_c,
# MAGIC   round(max(avg_tmpr_c), 2) as max_temperature_c,
# MAGIC   count(*) as records
# MAGIC from hotel_weather
# MAGIC group by wthr_date, city
# MAGIC order by city
