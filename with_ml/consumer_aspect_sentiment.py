from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, current_timestamp, udf, size
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, MapType
import psycopg2
from psycopg2.extras import execute_batch
import re
from textblob import TextBlob
import json

# --- Spark session ---
spark = SparkSession.builder \
    .appName("MastodonAspectSentimentConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Enhanced UDFs for Aspect-Based Sentiment ---
@udf(returnType=FloatType())
def sentiment_score(content):
    """Extract overall sentiment polarity using TextBlob"""
    if not content:
        return 0.0
    clean_text = re.sub(r'<[^>]+>', '', content)
    try:
        return float(TextBlob(clean_text).sentiment.polarity)
    except:
        return 0.0

@udf(returnType=MapType(StringType(), FloatType()))
def extract_aspect_sentiments(content):
    """
    Extract aspects (nouns/noun phrases) and their associated sentiments.
    Returns a map of aspect -> sentiment score
    """
    if not content:
        return {}
    
    clean_text = re.sub(r'<[^>]+>', '', content)
    
    try:
        blob = TextBlob(clean_text)
        aspect_sentiments = {}
        
        # Extract noun phrases as aspects
        for np in blob.noun_phrases:
            # Clean up the noun phrase
            aspect = np.strip().lower()
            if len(aspect) < 3 or len(aspect) > 50:  # Filter out very short/long phrases
                continue
            
            # Find sentences containing this aspect
            aspect_sentiment_scores = []
            for sentence in blob.sentences:
                if aspect in sentence.string.lower():
                    aspect_sentiment_scores.append(sentence.sentiment.polarity)
            
            # Average sentiment for this aspect
            if aspect_sentiment_scores:
                avg_sentiment = sum(aspect_sentiment_scores) / len(aspect_sentiment_scores)
                aspect_sentiments[aspect] = float(avg_sentiment)
        
        # Also extract hashtag-related aspects (topics mentioned with hashtags)
        hashtag_pattern = r'#(\w+)'
        hashtags_in_text = re.findall(hashtag_pattern, clean_text.lower())
        for tag in hashtags_in_text:
            if tag not in aspect_sentiments:
                # Find sentiment of sentences containing this hashtag
                tag_sentiments = []
                for sentence in blob.sentences:
                    if f'#{tag}' in sentence.string.lower():
                        tag_sentiments.append(sentence.sentiment.polarity)
                if tag_sentiments:
                    aspect_sentiments[tag] = float(sum(tag_sentiments) / len(tag_sentiments))
        
        return aspect_sentiments if aspect_sentiments else {}
        
    except Exception as e:
        print(f"Error in aspect extraction: {e}")
        return {}

@udf(returnType=StringType())
def extract_dominant_aspect(content):
    """Extract the most prominent aspect (most frequently mentioned noun phrase)"""
    if not content:
        return None
    
    clean_text = re.sub(r'<[^>]+>', '', content)
    
    try:
        blob = TextBlob(clean_text)
        noun_phrases = [np.strip().lower() for np in blob.noun_phrases if len(np.strip()) >= 3]
        
        if noun_phrases:
            # Return most common noun phrase
            from collections import Counter
            most_common = Counter(noun_phrases).most_common(1)
            return most_common[0][0] if most_common else None
        return None
    except:
        return None

@udf(returnType=StringType())
def categorize_sentiment(score):
    """Categorize sentiment into human-readable labels"""
    if score is None:
        return "neutral"
    score = float(score)
    if score > 0.5:
        return "very_positive"
    elif score > 0.1:
        return "positive"
    elif score < -0.5:
        return "very_negative"
    elif score < -0.1:
        return "negative"
    else:
        return "neutral"

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

# --- Parse JSON and Add Enhanced ML Features ---
enriched = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("sentiment", sentiment_score(col("content"))) \
    .withColumn("sentiment_category", categorize_sentiment(col("sentiment"))) \
    .withColumn("aspect_sentiments", extract_aspect_sentiments(col("content"))) \
    .withColumn("dominant_aspect", extract_dominant_aspect(col("content"))) \
    .withColumn("content_length", content_length_feature(col("content"))) \
    .withColumn("hashtag_count", size(col("hashtags"))) \
    .withColumn("processing_time", current_timestamp())

# --- Explode hashtags ---
exploded = enriched.withColumn("hashtag", explode(col("hashtags")))

# --- Write to PostgreSQL using psycopg2 ---
def write_to_postgres_batch(batch_df, batch_id):
    """Write batch with aspect-based sentiment to PostgreSQL"""
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
        
        # Prepare data for posts table
        posts_data = [
            (
                row.user_name, row.content, row.hashtag,
                row.sentiment, row.sentiment_category, row.dominant_aspect,
                row.content_length, row.hashtag_count,
                row.processing_time, row.timestamp
            ) for row in rows
        ]
        
        posts_query = """
            INSERT INTO posts (user_name, content, hashtag, sentiment, sentiment_category,
                             dominant_aspect, content_length, hashtag_count, 
                             processing_time, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        execute_batch(cur, posts_query, posts_data)
        
        # Insert aspect sentiments into separate table
        aspect_data = []
        for row in rows:
            if row.aspect_sentiments:
                for aspect, sentiment_val in row.aspect_sentiments.items():
                    aspect_data.append((
                        row.user_name,
                        row.hashtag,
                        aspect,
                        sentiment_val,
                        row.processing_time,
                        row.timestamp
                    ))
        
        if aspect_data:
            aspect_query = """
                INSERT INTO aspect_sentiments (user_name, hashtag, aspect, sentiment, 
                                              processing_time, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            execute_batch(cur, aspect_query, aspect_data)
        
        conn.commit()
        print(f"✓ Batch {batch_id}: Inserted {len(rows)} posts and {len(aspect_data)} aspects into PostgreSQL")
        
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
    .option("checkpointLocation", "/tmp/spark-checkpoints-kafka-aspect") \
    .outputMode("append") \
    .start()

# --- Write to PostgreSQL ---
postgres_query = exploded.writeStream \
    .foreachBatch(write_to_postgres_batch) \
    .option("checkpointLocation", "/tmp/spark-checkpoints-postgres-aspect") \
    .outputMode("append") \
    .start()

print("✓ Spark streaming with aspect-based sentiment started")
spark.streams.awaitAnyTermination()