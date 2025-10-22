from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import psycopg2
from psycopg2.extras import execute_batch

# --- Spark session ---
spark = SparkSession.builder \
    .appName("MastodonConsumerSimple") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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

# --- Add processing timestamp ---
parsed = parsed.withColumn("processing_time", current_timestamp())

# --- Explode hashtags ---
exploded = parsed.withColumn("hashtag", explode(col("hashtags")))

# --- Write to PostgreSQL using psycopg2 (more reliable) ---
def write_to_postgres_batch(batch_df, batch_id):
    """Write batch to PostgreSQL using psycopg2"""
    
    # Collect rows (for small batches this is OK)
    rows = batch_df.collect()
    
    if not rows:
        return
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            database="mastodon_analytics",
            user="mastodon",
            password="your_password"
        )
        cur = conn.cursor()
        
        # Prepare data for batch insert
        data_to_insert = []
        for row in rows:
            data_to_insert.append((
                row.user_name,
                row.content,
                row.hashtag,
                None,  # sentiment (will be added later)
                None,  # content_length (will be added later)
                None,  # hashtag_count (will be added later)
                row.processing_time,
                row.timestamp
            ))
        
        # Batch insert
        insert_query = """
            INSERT INTO posts (user_name, content, hashtag, sentiment, content_length, 
                             hashtag_count, processing_time, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        execute_batch(cur, insert_query, data_to_insert)
        conn.commit()
        
        print(f"✓ Batch {batch_id}: Inserted {len(rows)} rows into PostgreSQL")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"✗ Batch {batch_id}: Error writing to PostgreSQL: {e}")
        if 'conn' in locals():
            conn.rollback()
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

print("✓ Spark streaming started")
print("  → Reading from: mastodon-raw")
print("  → Writing to: mastodon-processed (Kafka)")
print("  → Writing to: posts table (PostgreSQL)")

# Wait for termination
spark.streams.awaitAnyTermination()