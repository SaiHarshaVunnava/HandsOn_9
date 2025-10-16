# Import the necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1️⃣ Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics_Task3").getOrCreate()

# 2️⃣ Define schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# 3️⃣ Read streaming data from socket
data = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# 4️⃣ Parse JSON data into structured columns
parsed_data = (
    data.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
)

# 5️⃣ Convert timestamp column to TimestampType and add a watermark
data_with_timestamp = parsed_data.withColumn("event_time", col("timestamp").cast(TimestampType()))
data_with_watermark = data_with_timestamp.withWatermark("event_time", "1 minute")

# 6️⃣ Perform 5-min windowed aggregation sliding by 1 min
data_aggregations = (
    data_with_watermark
    .groupBy(window(col("event_time"), "5 minutes", "1 minute"))
    .agg(_sum("fare_amount").alias("total_fare"))
)

# 7️⃣ Extract window start and end columns
final_data = (
    data_aggregations.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_fare")
    )
)

# 8️⃣ Define a function to write each batch to CSV (inside outputs/task3/)
def write_batch_to_csv(df, batch_id):
    output_path = f"outputs/task3/batch_{batch_id}"
    df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

# 9️⃣ Use foreachBatch for micro-batch writing
query_task3 = (
    final_data.writeStream
    .foreachBatch(write_batch_to_csv)
    .outputMode("append")
    .option("checkpointLocation", "checkpoints/task3/")
    .start()
)

# 🔟 Keep the stream active
query_task3.awaitTermination()