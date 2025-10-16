from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1️⃣ Create Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics_Task1").getOrCreate()

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

# 5️⃣ Write parsed data to CSV files inside outputs/task1/
query_task1 = (
    parsed_data.writeStream
    .format("csv")
    .outputMode("append")
    .option("header", "true")
    .option("path", "outputs/task1/")                     # 👈 matches your structure
    .option("checkpointLocation", "checkpoints/task1/")   # 👈 checkpoint folder inside checkpoints/
    .start()
)

# 6️⃣ Keep the stream running
query_task1.awaitTermination()