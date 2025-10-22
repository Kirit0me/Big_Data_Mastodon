import pandas as pd
import psycopg2
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
import joblib
import logging
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MastodonMLService:
    def __init__(self):
        self.db_conn = psycopg2.connect(
            dbname="mastodon_analytics",
            user="mastodon",
            password="your_password",
            host="localhost"
        )
        self.create_tables()
        self.scaler = StandardScaler()
        
    def create_tables(self):
        """Create tables for ML results"""
        with self.db_conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_clusters (
                    user_name VARCHAR(255) PRIMARY KEY,
                    cluster_id INT,
                    avg_sentiment FLOAT,
                    avg_content_length FLOAT,
                    post_count INT,
                    unique_hashtags INT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trending_predictions (
                    hashtag VARCHAR(255),
                    prediction_score FLOAT,
                    current_velocity FLOAT,
                    predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (hashtag, predicted_at)
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS anomaly_detection (
                    id SERIAL PRIMARY KEY,
                    user_name VARCHAR(255),
                    hashtag VARCHAR(255),
                    anomaly_score FLOAT,
                    reason TEXT,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.db_conn.commit()
    
    def fetch_user_features(self):
        """Fetch aggregated user features for clustering"""
        query = """
            SELECT 
                user_name,
                AVG(sentiment) as avg_sentiment,
                AVG(content_length) as avg_content_length,
                COUNT(*) as post_count,
                COUNT(DISTINCT hashtag) as unique_hashtags
            FROM posts
            WHERE processing_time > NOW() - INTERVAL '7 days'
            GROUP BY user_name
            HAVING COUNT(*) >= 5
        """
        
        df = pd.read_sql(query, self.db_conn)
        logger.info(f"Fetched features for {len(df)} users")
        return df
    
    def cluster_users(self):
        """Cluster users based on their posting behavior"""
        logger.info("Starting user clustering...")
        
        df = self.fetch_user_features()
        if len(df) < 10:
            logger.warning("Not enough users for clustering")
            return
        
        # Prepare features
        features = ['avg_sentiment', 'avg_content_length', 'post_count', 'unique_hashtags']
        X = df[features].fillna(0)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Determine optimal k using elbow method (simplified)
        n_clusters = min(5, len(df) // 10)
        
        # Perform clustering
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        df['cluster_id'] = kmeans.fit_predict(X_scaled)
        
        # Save to database
        with self.db_conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO user_clusters 
                    (user_name, cluster_id, avg_sentiment, avg_content_length, 
                     post_count, unique_hashtags, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (user_name) 
                    DO UPDATE SET 
                        cluster_id = EXCLUDED.cluster_id,
                        avg_sentiment = EXCLUDED.avg_sentiment,
                        avg_content_length = EXCLUDED.avg_content_length,
                        post_count = EXCLUDED.post_count,
                        unique_hashtags = EXCLUDED.unique_hashtags,
                        updated_at = NOW()
                """, (row['user_name'], int(row['cluster_id']), 
                      row['avg_sentiment'], row['avg_content_length'],
                      int(row['post_count']), int(row['unique_hashtags'])))
            self.db_conn.commit()
        
        logger.info(f"Clustered users into {n_clusters} groups")
    
    def predict_trending_hashtags(self):
        """Predict which hashtags are trending using velocity"""
        logger.info("Predicting trending hashtags...")
        
        query = """
            WITH hourly_counts AS (
                SELECT 
                    hashtag,
                    DATE_TRUNC('hour', processing_time) as hour,
                    COUNT(*) as count
                FROM posts
                WHERE processing_time > NOW() - INTERVAL '24 hours'
                GROUP BY hashtag, DATE_TRUNC('hour', processing_time)
            ),
            velocity AS (
                SELECT 
                    hashtag,
                    AVG(count) as avg_count,
                    STDDEV(count) as stddev_count,
                    (MAX(count) - MIN(count)) / NULLIF(COUNT(*), 0) as velocity
                FROM hourly_counts
                GROUP BY hashtag
                HAVING COUNT(*) >= 3
            )
            SELECT * FROM velocity
            ORDER BY velocity DESC
            LIMIT 50
        """
        
        df = pd.read_sql(query, self.db_conn)
        
        if len(df) == 0:
            logger.warning("No trending data available")
            return
        
        # Simple prediction score: velocity * avg_count
        df['prediction_score'] = df['velocity'] * df['avg_count']
        
        # Normalize scores
        df['prediction_score'] = (df['prediction_score'] - df['prediction_score'].min()) / \
                                  (df['prediction_score'].max() - df['prediction_score'].min())
        
        # Save predictions
        with self.db_conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO trending_predictions 
                    (hashtag, prediction_score, current_velocity, predicted_at)
                    VALUES (%s, %s, %s, NOW())
                """, (row['hashtag'], float(row['prediction_score']), float(row['velocity'])))
            self.db_conn.commit()
        
        logger.info(f"Saved predictions for {len(df)} hashtags")
    
    def detect_anomalies(self):
        """Detect unusual posting patterns"""
        logger.info("Detecting anomalies...")
        
        query = """
            WITH user_stats AS (
                SELECT 
                    user_name,
                    COUNT(*) as recent_posts,
                    AVG(sentiment) as avg_sentiment,
                    COUNT(DISTINCT hashtag) as unique_tags
                FROM posts
                WHERE processing_time > NOW() - INTERVAL '1 hour'
                GROUP BY user_name
            ),
            historical_stats AS (
                SELECT 
                    user_name,
                    AVG(post_count_per_hour) as avg_posts_per_hour,
                    STDDEV(post_count_per_hour) as stddev_posts
                FROM (
                    SELECT 
                        user_name,
                        DATE_TRUNC('hour', processing_time) as hour,
                        COUNT(*) as post_count_per_hour
                    FROM posts
                    WHERE processing_time > NOW() - INTERVAL '7 days'
                    GROUP BY user_name, DATE_TRUNC('hour', processing_time)
                ) h
                GROUP BY user_name
            )
            SELECT 
                u.user_name,
                u.recent_posts,
                h.avg_posts_per_hour,
                h.stddev_posts,
                (u.recent_posts - h.avg_posts_per_hour) / NULLIF(h.stddev_posts, 0) as z_score
            FROM user_stats u
            JOIN historical_stats h ON u.user_name = h.user_name
            WHERE h.stddev_posts > 0
        """
        
        df = pd.read_sql(query, self.db_conn)
        
        # Flag anomalies (z-score > 3)
        anomalies = df[abs(df['z_score']) > 3]
        
        if len(anomalies) > 0:
            with self.db_conn.cursor() as cur:
                for _, row in anomalies.iterrows():
                    reason = f"Unusual posting rate: {row['recent_posts']} posts (avg: {row['avg_posts_per_hour']:.1f})"
                    cur.execute("""
                        INSERT INTO anomaly_detection 
                        (user_name, anomaly_score, reason, detected_at)
                        VALUES (%s, %s, %s, NOW())
                    """, (row['user_name'], float(abs(row['z_score'])), reason))
                self.db_conn.commit()
            
            logger.info(f"Detected {len(anomalies)} anomalies")
    
    def run_periodic_training(self, interval_seconds=600):
        """Run ML tasks periodically"""
        logger.info(f"Starting periodic ML training (every {interval_seconds}s)")
        
        while True:
            try:
                self.cluster_users()
                self.predict_trending_hashtags()
                self.detect_anomalies()
                
                logger.info(f"ML training complete. Sleeping for {interval_seconds}s...")
                time.sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"ML training failed: {e}")
                time.sleep(60)

if __name__ == "__main__":
    ml_service = MastodonMLService()
    ml_service.run_periodic_training(interval_seconds=600)  # Run every 10 minutes