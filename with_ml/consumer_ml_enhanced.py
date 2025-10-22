from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, current_timestamp, udf, size
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
import psycopg2
from psycopg2.extras import execute_batch
import re
from textblob import TextBlob

# --- Spark session ---
spark = SparkSession.builder \
    .appName("MastodonConsumerEnhanced") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- UDFs for ML features ---
@udf(returnType=FloatType())
def sentiment_score(content):
    """Extract sentiment polarity using TextBlob"""
    if not content:
        return 0.0
    clean_text = re.sub(r'<[^>]+>', '', content) # Remove HTML tags
    try:
        return float(TextBlob(clean_text).sentiment.polarity)
    except:
        return 0.0

@udf(returnType=FloatType())
def content_length_feature(content):
    """Calculate content length as feature"""
    if not content:
        return 0.0
    clean_text = re.sub(r'<[^>]+>', '', content) # Remove HTML tags
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

# --- Parse JSON and Add ML Features ---
enriched = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("sentiment", sentiment_score(col("content"))) \
    .withColumn("content_length", content_length_feature(col("content"))) \
    .withColumn("hashtag_count", size(col("hashtags"))) \
    .withColumn("processing_time", current_timestamp())

# --- Explode hashtags ---
exploded = enriched.withColumn("hashtag", explode(col("hashtags")))

# --- Write to PostgreSQL using psycopg2 ---
def write_to_postgres_batch(batch_df, batch_id):
    """Write batch with ML features to PostgreSQL"""
    rows = batch_df.collect()
    if not rows:
        return
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="mastodon_analytics",
            user="mastodon",
            password="your_password"
        )
        cur = conn.cursor()
        
        # Prepare data with ML features for batch insert
        data_to_insert = [
            (
                row.user_name, row.content, row.hashtag,
                row.sentiment, row.content_length, row.hashtag_count,
                row.processing_time, row.timestamp
            ) for row in rows
        ]
        
        insert_query = """
            INSERT INTO posts (user_name, content, hashtag, sentiment, content_length, 
                             hashtag_count, processing_time, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        execute_batch(cur, insert_query, data_to_insert)
        conn.commit()
        print(f"✓ Batch {batch_id}: Inserted {len(rows)} enriched rows into PostgreSQL")
        
    except Exception as e:
        print(f"✗ Batch {batch_id}: Error writing to PostgreSQL: {e}")
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

# --- Write to Kafka (processed topic) ---
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
    .foreachBatch(write_to_postgres_batch) \
    .option("checkpointLocation", "/tmp/spark-checkpoints-postgres") \
    .outputMode("append") \
    .start()

print("✓ Spark streaming with ML features started")
spark.streams.awaitAnyTermination()