from pyspark.sql import SparkSession
from pyspark.sql.functions import (from_json, col, explode, window, count, 
                                   collect_list, udf, current_timestamp, length, size)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
import re
from textblob import TextBlob

# --- Spark session ---
spark = SparkSession.builder \
    .appName("MastodonMLConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- UDFs for ML features ---
@udf(returnType=FloatType())
def sentiment_score(content):
    """Extract sentiment polarity using TextBlob"""
    if not content:
        return 0.0
    # Remove HTML tags
    clean_text = re.sub(r'<[^>]+>', '', content)
    try:
        return float(TextBlob(clean_text).sentiment.polarity)
    except:
        return 0.0

@udf(returnType=FloatType())
def content_length_feature(content):
    """Calculate content length as feature"""
    if not content:
        return 0.0
    clean_text = re.sub(r'<[^>]+>', '', content)
    return float(len(clean_text))

# --- Schema for raw messages ---
schema = StructType([
    StructField("user_name", StringType()),
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

# --- Add ML features ---
enriched = parsed \
    .withColumn("sentiment", sentiment_score(col("content"))) \
    .withColumn("content_length", content_length_feature(col("content"))) \
    .withColumn("hashtag_count", size(col("hashtags"))) \
    .withColumn("processing_time", current_timestamp())

# --- Explode hashtags ---
exploded = enriched.withColumn("hashtag", explode(col("hashtags")))

# --- Write to PostgreSQL (for Grafana) ---
def write_to_postgres(batch_df, batch_id):
    """Write each micro-batch to PostgreSQL"""
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/mastodon_analytics") \
        .option("dbtable", "posts") \
        .option("user_name", "mastodon") \
        .option("password", "your_password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# --- Write to processed Kafka topic ---
kafka_query = exploded.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "mastodon-processed") \
    .option("checkpointLocation", "/tmp/spark-checkpoints-kafka") \
    .outputMode("append") \
    .start()

# --- Write to PostgreSQL ---
postgres_query = exploded.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/spark-checkpoints-postgres") \
    .outputMode("append") \
    .start()

# Wait for both streams
spark.streams.awaitAnyTermination()