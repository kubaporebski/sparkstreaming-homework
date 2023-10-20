# Databricks notebook source
import threading
import time 
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/checkpoints/", True)
dbutils.fs.mkdirs("dbfs:/tmp/checkpoints/")

spark.sql("drop table if exists hotels_by_city_date")
spark.sql("drop table if exists hotels_temperature_stats_by_city_date")
spark.sql("drop table if exists top10_cities")


# COMMAND ----------


hw_stream = spark.readStream \
    .format("cloudFiles") \
    .schema("address string, avg_tmpr_c double, avg_tmpr_f double, city string, country string, geoHash string, id string, latitude double, longitude double, name string, wthr_date string, wthr_year string, wthr_month string, wthr_day string") \
    .option("cloudFiles.format", "parquet") \
    .load("gs://storage-bucket-select-gar/m13sparkstreaming/hotel-weather")



# COMMAND ----------

spark_streams = []

spark_streams.append(
    hw_stream
        .groupBy(F.col("city"), F.col("wthr_date")) 
        .agg(F.approx_count_distinct(F.col("id")).alias("distinct_hotels_by_city")) 
        .writeStream 
        .queryName("hotels_by_city_date") 
        .format("delta") 
        .outputMode("complete")
        .option("checkpointLocation", "/tmp/checkpoints/hotels_by_city_date")
        .toTable("hotels_by_city_date")
)

spark_streams.append(
    hw_stream
        .groupBy(F.col("city"), F.col("wthr_date")).agg(
            F.round(F.min("avg_tmpr_c"), 2).alias("min_temperature_c"),
            F.round(F.avg("avg_tmpr_c"), 2).alias("mean_temperature_c"),
            F.round(F.max("avg_tmpr_c"), 2).alias("max_temperature_c")) 
        .writeStream 
        .queryName("hotel_weather_stats") 
        .format("delta") 
        .outputMode("complete") 
        .option("checkpointLocation", "/tmp/checkpoints/hotels_temperature_stats_by_city_date")
        .toTable("hotels_temperature_stats_by_city_date")
)

spark_streams.append(
    hw_stream
        .groupBy(F.col("city"))
        .agg(F.approx_count_distinct(F.col("id")).alias("hotels_count"))
        .orderBy(F.col("hotels_count").desc())
        .limit(10)
        .writeStream
        .queryName("top10_cities")
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", "/tmp/checkpoints/top10_cities")
        .toTable("top10_cities")
)

def stop_streams(streams):
    print("Waiting 120 seconds... ")
    time.sleep(120)
    print("Stopping... ")
    for stream in streams:
        stream.stop()


thr = threading.Thread(target=stop_streams, args=[spark_streams], daemon=True)
thr.start()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hotels_by_city_date 
# MAGIC order by wthr_date, distinct_hotels_by_city desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hotels_temperature_stats_by_city_date 
# MAGIC order by wthr_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from top10_cities
