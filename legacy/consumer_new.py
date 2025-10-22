from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType

# --- Spark session ---
spark = SparkSession.builder \
    .appName("MastodonConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Schema for raw messages ---
schema = StructType([
    StructField("user", StringType()),
    StructField("content", StringType()),
    StructField("hashtags", ArrayType(StringType())),
    StructField("timestamp", StringType()),
])

# --- Read from raw topic ---
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "mastodon-raw") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# --- Parse JSON ---
parsed = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# --- Explode hashtags/tags ---
exploded = parsed.withColumn("hashtag", explode(col("hashtags")))


# --- Write to processed topic ---
query = exploded.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "mastodon-processed") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .outputMode("append") \
    .start()

query.awaitTermination()
