"""
Enhanced Spark Consumer with Aspect-Based Sentiment Analysis
This replaces your existing consumer_ml_enhanced.py completely
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, current_timestamp, udf, size, length
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
import psycopg2
from psycopg2.extras import execute_batch
import re
import os
import sys

# Add current directory to path for imports
sys.path.insert(0, '/app')

# Import TextBlob for sentiment
try:
    from textblob import TextBlob
except ImportError:
    print("WARNING: TextBlob not available, using simple sentiment")
    TextBlob = None

# Import spaCy for aspect extraction
try:
    import spacy
    nlp = spacy.load("en_core_web_sm")
    SPACY_AVAILABLE = True
except:
    print("WARNING: spaCy not available, aspect extraction disabled")
    SPACY_AVAILABLE = False
    nlp = None

# --- Configuration from Environment Variables ---
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'mastodon_analytics')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'mastodon')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'your_password')

print(f"Configuration:")
print(f"  Kafka: {KAFKA_BOOTSTRAP}")
print(f"  PostgreSQL: {POSTGRES_USER}@{POSTGRES_HOST}/{POSTGRES_DB}")
print(f"  spaCy Available: {SPACY_AVAILABLE}")

# --- Initialize Spark Session ---
spark = SparkSession.builder \
    .appName("MastodonConsumerEnhanced") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Helper Functions for Sentiment Analysis ---

def clean_html(text):
    """Remove HTML tags from text"""
    if not text:
        return ""
    return re.sub(r'<[^>]+>', '', text)

def extract_sentiment_simple(text):
    """Simple sentiment without TextBlob"""
    if not text:
        return 0.0
    
    # Very basic sentiment based on common words
    positive_words = ['love', 'great', 'awesome', 'excellent', 'amazing', 'wonderful', 'fantastic']
    negative_words = ['hate', 'terrible', 'awful', 'bad', 'horrible', 'worst', 'sucks']
    
    text_lower = text.lower()
    pos_count = sum(1 for word in positive_words if word in text_lower)
    neg_count = sum(1 for word in negative_words if word in text_lower)
    
    if pos_count + neg_count == 0:
        return 0.0
    
    return (pos_count - neg_count) / (pos_count + neg_count)

def extract_aspects_simple(text):
    """Simple aspect extraction without spaCy"""
    if not text or len(text) < 5:
        return None
    
    # Extract words that might be aspects (capitalized words, common nouns)
    words = text.split()
    aspects = []
    
    for word in words:
        # Look for capitalized words (might be product names, people, etc.)
        clean_word = re.sub(r'[^\w\s]', '', word)
        if clean_word and len(clean_word) > 3:
            if clean_word[0].isupper() and clean_word not in ['The', 'This', 'That']:
                aspects.append(clean_word.lower())
    
    if aspects:
        return aspects[0]  # Return first aspect found
    return None

def extract_aspects_spacy(text):
    """Extract aspects using spaCy"""
    if not text or not nlp:
        return None
    
    try:
        doc = nlp(text)
        
        # Named entities (people, organizations, products)
        for ent in doc.ents:
            if ent.label_ in ['PERSON', 'ORG', 'PRODUCT', 'GPE']:
                return ent.text.lower()
        
        # Important noun phrases
        for chunk in doc.noun_chunks:
            if len(chunk.text) > 3 and chunk.root.pos_ == 'NOUN':
                return chunk.text.lower()
    except:
        pass
    
    return None

# --- Define UDFs for Sentiment Analysis ---

# Schema for sentiment result
sentiment_schema = StructType([
    StructField("overall_sentiment", FloatType()),
    StructField("overall_subjectivity", FloatType()),
    StructField("sentiment_summary", StringType()),
    StructField("top_aspect", StringType()),
    StructField("top_aspect_sentiment", FloatType()),
])

@udf(returnType=sentiment_schema)
def analyze_sentiment_with_aspects(content, hashtags):
    """
    Analyze sentiment with aspect extraction
    Returns: (overall_sentiment, subjectivity, summary, top_aspect, aspect_sentiment)
    """
    if not content:
        return (0.0, 0.0, "No content", None, 0.0)
    
    try:
        # Clean HTML
        clean_text = clean_html(content)
        
        if len(clean_text) < 5:
            return (0.0, 0.0, "Content too short", None, 0.0)
        
        # Calculate overall sentiment
        if TextBlob:
            blob = TextBlob(clean_text)
            overall_sentiment = float(blob.sentiment.polarity)
            overall_subjectivity = float(blob.sentiment.subjectivity)
        else:
            overall_sentiment = extract_sentiment_simple(clean_text)
            overall_subjectivity = 0.5
        
        # Extract top aspect
        if SPACY_AVAILABLE:
            top_aspect = extract_aspects_spacy(clean_text)
        else:
            top_aspect = extract_aspects_simple(clean_text)
        
        # If we found an aspect, calculate its sentiment
        if top_aspect:
            # Find sentences containing the aspect
            sentences = clean_text.split('.')
            aspect_sentences = [s for s in sentences if top_aspect in s.lower()]
            
            if aspect_sentences and TextBlob:
                # Calculate average sentiment of aspect sentences
                aspect_sentiment = sum(
                    TextBlob(s).sentiment.polarity 
                    for s in aspect_sentences
                ) / len(aspect_sentences)
            else:
                aspect_sentiment = overall_sentiment
        else:
            aspect_sentiment = overall_sentiment
        
        # Generate summary
        if overall_sentiment > 0.3:
            tone = "Positive"
        elif overall_sentiment < -0.3:
            tone = "Negative"
        else:
            tone = "Neutral"
        
        if top_aspect:
            summary = f"{tone} tone; focused on '{top_aspect}' ({aspect_sentiment:.2f})"
        else:
            summary = f"{tone} tone overall"
        
        return (
            float(overall_sentiment),
            float(overall_subjectivity),
            summary,
            top_aspect,
            float(aspect_sentiment)
        )
        
    except Exception as e:
        print(f"Error in sentiment analysis: {e}")
        return (0.0, 0.0, f"Error: {str(e)}", None, 0.0)

# Simple content length UDF
@udf(returnType=FloatType())
def calculate_content_length(content):
    """Calculate cleaned content length"""
    if not content:
        return 0.0
    clean_text = clean_html(content)
    return float(len(clean_text))

# --- Schema for incoming Kafka messages ---
schema = StructType([
    StructField("user_name", StringType()),
    StructField("content", StringType()),
    StructField("hashtags", ArrayType(StringType())),
    StructField("timestamp", StringType()),
])

# --- Read from Kafka raw topic ---
print(f"📡 Connecting to Kafka topic: mastodon-raw")
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "mastodon-raw") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# --- Parse JSON and add ML features ---
print("🔄 Setting up streaming pipeline with aspect-based sentiment...")

enriched = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("sentiment_analysis", analyze_sentiment_with_aspects(col("content"), col("hashtags"))) \
    .withColumn("overall_sentiment", col("sentiment_analysis.overall_sentiment")) \
    .withColumn("overall_subjectivity", col("sentiment_analysis.overall_subjectivity")) \
    .withColumn("sentiment_summary", col("sentiment_analysis.sentiment_summary")) \
    .withColumn("top_aspect", col("sentiment_analysis.top_aspect")) \
    .withColumn("top_aspect_sentiment", col("sentiment_analysis.top_aspect_sentiment")) \
    .withColumn("content_length", calculate_content_length(col("content"))) \
    .withColumn("hashtag_count", size(col("hashtags"))) \
    .withColumn("processing_time", current_timestamp()) \
    .drop("sentiment_analysis")  # Drop the struct, we've extracted all fields

# --- Explode hashtags (one row per hashtag) ---
exploded = enriched.withColumn("hashtag", explode(col("hashtags")))

# --- Write to PostgreSQL function ---
def write_to_postgres_batch(batch_df, batch_id):
    """Write each batch to PostgreSQL with enhanced sentiment features"""
    rows = batch_df.collect()
    
    if not rows:
        print(f"⚠️  Batch {batch_id}: No rows to process")
        return
    
    conn = None
    cur = None
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        
        # Prepare data for batch insert
        data_to_insert = []
        for row in rows:
            data_to_insert.append((
                row.user_name,
                row.content,
                row.hashtag,
                float(row.overall_sentiment) if row.overall_sentiment else 0.0,
                float(row.content_length) if row.content_length else 0.0,
                int(row.hashtag_count) if row.hashtag_count else 0,
                row.processing_time,
                row.timestamp,
                row.sentiment_summary,
                row.top_aspect,
                float(row.top_aspect_sentiment) if row.top_aspect_sentiment else 0.0
            ))
        
        # Batch insert query with enhanced fields
        insert_query = """
            INSERT INTO posts (
                user_name, content, hashtag, 
                sentiment, content_length, hashtag_count,
                processing_time, timestamp,
                sentiment_summary, top_aspect, top_aspect_sentiment
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        execute_batch(cur, insert_query, data_to_insert, page_size=100)
        conn.commit()
        
        print(f"✅ Batch {batch_id}: Inserted {len(rows)} rows with aspect-based sentiment")
        
        # Show sample of what was inserted
        if len(data_to_insert) > 0:
            sample = data_to_insert[0]
            print(f"   Sample: user={sample[0]}, sentiment={sample[3]:.2f}, "
                  f"aspect={sample[9]}, aspect_sentiment={sample[10]:.2f}")
        
    except Exception as e:
        print(f"❌ Batch {batch_id}: Error writing to PostgreSQL: {e}")
        if conn:
            conn.rollback()
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Write enriched data back to Kafka (processed topic) ---
print("📤 Setting up Kafka output stream (mastodon-processed)...")
kafka_query = exploded.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("topic", "mastodon-processed") \
    .option("checkpointLocation", "/tmp/spark-checkpoints-kafka") \
    .outputMode("append") \
    .start()

# --- Write to PostgreSQL ---
print("💾 Setting up PostgreSQL output stream...")
postgres_query = exploded.writeStream \
    .foreachBatch(write_to_postgres_batch) \
    .option("checkpointLocation", "/tmp/spark-checkpoints-postgres") \
    .outputMode("append") \
    .start()

print("")
print("=" * 60)
print("✅ Spark Streaming with Aspect-Based Sentiment STARTED")
print("=" * 60)
print("")
print("📊 Processing:")
print("   • Reading from: mastodon-raw")
print("   • Writing to: mastodon-processed (Kafka)")
print("   • Writing to: PostgreSQL (posts table)")
print("")
print("🎯 Enhanced Features:")
print("   • Overall sentiment analysis")
print("   • Aspect/topic extraction")
print("   • Aspect-specific sentiment")
print("   • Sentiment summaries")
print("")
print("Press Ctrl+C to stop...")
print("")

# Wait for termination
spark.streams.awaitAnyTermination()