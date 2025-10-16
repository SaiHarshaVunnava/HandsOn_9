from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1️⃣ Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics_Task2").getOrCreate()

# 2️⃣ Define the schema for incoming JSON data
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

# 4️⃣ Parse JSON data into columns using the defined schema
parsed_data = (
    data.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
)

# 5️⃣ Convert timestamp column to TimestampType and add a watermark
data_with_timestamp = parsed_data.withColumn("event_time", col("timestamp").cast(TimestampType()))
data_with_watermark = data_with_timestamp.withWatermark("event_time", "1 minute")

# 6️⃣ Compute aggregations: total fare and average distance grouped by driver_id
data_aggregations = (
    data_with_watermark
    .groupBy("driver_id")
    .agg(
        _sum("fare_amount").alias("total_fare"),
        avg("distance_km").alias("avg_distance")
    )
)

# 7️⃣ Define a function to write each batch to a CSV file inside outputs/task2/
def write_batch_to_csv(df, batch_id):
    output_path = f"outputs/task2/batch_{batch_id}"
    df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

# 8️⃣ Use foreachBatch to apply the function to each micro-batch
query_task2 = (
    data_aggregations.writeStream
    .foreachBatch(write_batch_to_csv)
    .outputMode("complete")
    .option("checkpointLocation", "checkpoints/task2/")
    .start()
)

# 9️⃣ Keep the stream running
query_task2.awaitTermination()