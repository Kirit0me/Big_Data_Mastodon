-- ============================================================================
-- Complete Database Schema for Mastodon Analytics with Aspect-Based Sentiment
-- ============================================================================
-- This script creates all tables, indexes, views, and grants privileges
-- Run this on a fresh PostgreSQL database to set up the complete system
-- ============================================================================

-- Drop existing objects if they exist (for clean reinstall)
DROP MATERIALIZED VIEW IF EXISTS aspect_stats CASCADE;
DROP MATERIALIZED VIEW IF EXISTS hashtag_stats CASCADE;
DROP VIEW IF EXISTS hashtag_aspect_sentiment CASCADE;
DROP TABLE IF EXISTS anomaly_detection CASCADE;
DROP TABLE IF EXISTS trending_predictions CASCADE;
DROP TABLE IF EXISTS user_clusters CASCADE;
DROP TABLE IF EXISTS user_hashtag_network CASCADE;
DROP TABLE IF EXISTS hashtag_communities CASCADE;
DROP TABLE IF EXISTS hashtag_centrality CASCADE;
DROP TABLE IF EXISTS aspect_sentiments CASCADE;
DROP TABLE IF EXISTS posts CASCADE;

-- ============================================================================
-- MAIN TABLES
-- ============================================================================

-- Main posts table with ML features and aspect-based sentiment
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_name VARCHAR(255) NOT NULL,
    content TEXT,
    hashtag VARCHAR(255) NOT NULL,
    sentiment FLOAT,                      -- Overall sentiment score (-1 to +1)
    sentiment_category VARCHAR(50),       -- Categorized: very_positive, positive, neutral, negative, very_negative
    dominant_aspect VARCHAR(255),         -- Main topic/aspect discussed in the post
    content_length FLOAT,                 -- Character count (ML feature)
    hashtag_count INT,                    -- Number of hashtags in post (ML feature)
    processing_time TIMESTAMP NOT NULL,   -- When the post was processed by our pipeline
    timestamp TIMESTAMP NOT NULL,         -- Original post timestamp from Mastodon
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for posts table
CREATE INDEX idx_posts_processing_time ON posts(processing_time);
CREATE INDEX idx_posts_hashtag ON posts(hashtag);
CREATE INDEX idx_posts_user ON posts(user_name);
CREATE INDEX idx_posts_sentiment_category ON posts(sentiment_category);
CREATE INDEX idx_posts_dominant_aspect ON posts(dominant_aspect);
CREATE INDEX idx_posts_timestamp ON posts(timestamp);

-- Aspect sentiments table - stores detailed sentiment for each aspect/topic
CREATE TABLE aspect_sentiments (
    id SERIAL PRIMARY KEY,
    user_name VARCHAR(255) NOT NULL,
    hashtag VARCHAR(255) NOT NULL,
    aspect VARCHAR(255) NOT NULL,         -- Specific topic/entity mentioned (e.g., "climate change", "new iphone")
    sentiment FLOAT NOT NULL,             -- Sentiment towards this specific aspect
    processing_time TIMESTAMP NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for aspect_sentiments table
CREATE INDEX idx_aspect_sentiments_aspect ON aspect_sentiments(aspect);
CREATE INDEX idx_aspect_sentiments_hashtag ON aspect_sentiments(hashtag);
CREATE INDEX idx_aspect_sentiments_time ON aspect_sentiments(processing_time);
CREATE INDEX idx_aspect_sentiments_user ON aspect_sentiments(user_name);
CREATE INDEX idx_aspect_sentiments_sentiment ON aspect_sentiments(sentiment);

-- ============================================================================
-- GRAPH ANALYTICS TABLES
-- ============================================================================

-- Hashtag centrality metrics from graph analysis
CREATE TABLE hashtag_centrality (
    hashtag VARCHAR(255) PRIMARY KEY,
    degree_centrality FLOAT,              -- How many other hashtags it connects to
    betweenness_centrality FLOAT,         -- How often it bridges different communities
    pagerank FLOAT,                       -- Overall importance in the network
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Community detection results
CREATE TABLE hashtag_communities (
    hashtag VARCHAR(255),
    community_id INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (hashtag, community_id)
);

-- User-hashtag network (co-occurrence tracking)
CREATE TABLE user_hashtag_network (
    user_name VARCHAR(255),
    hashtag VARCHAR(255),
    co_occurrence_count INT DEFAULT 1,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_name, hashtag)
);

-- ============================================================================
-- MACHINE LEARNING TABLES
-- ============================================================================

-- User clusters from ML clustering algorithm
CREATE TABLE user_clusters (
    user_name VARCHAR(255) PRIMARY KEY,
    cluster_id INT NOT NULL,
    avg_sentiment FLOAT,
    avg_content_length FLOAT,
    post_count INT,
    unique_hashtags INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trending predictions from ML models
CREATE TABLE trending_predictions (
    hashtag VARCHAR(255),
    prediction_score FLOAT NOT NULL,      -- 0-1 score indicating trending probability
    current_velocity FLOAT,               -- Rate of growth
    predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (hashtag, predicted_at)
);

-- Anomaly detection results
CREATE TABLE anomaly_detection (
    id SERIAL PRIMARY KEY,
    user_name VARCHAR(255),
    hashtag VARCHAR(255),
    anomaly_score FLOAT NOT NULL,         -- Z-score or similar anomaly metric
    reason TEXT,                          -- Human-readable explanation
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for anomaly detection
CREATE INDEX idx_anomaly_detection_detected_at ON anomaly_detection(detected_at);
CREATE INDEX idx_anomaly_detection_user ON anomaly_detection(user_name);

-- ============================================================================
-- MATERIALIZED VIEWS (for fast aggregations)
-- ============================================================================

-- Hashtag statistics (updated periodically)
CREATE MATERIALIZED VIEW hashtag_stats AS
SELECT 
    hashtag,
    COUNT(*) as total_posts,
    AVG(sentiment) as avg_sentiment,
    COUNT(DISTINCT user_name) as unique_users,
    MAX(processing_time) as last_seen,
    COUNT(DISTINCT dominant_aspect) as unique_aspects,
    MIN(processing_time) as first_seen
FROM posts
GROUP BY hashtag;

CREATE UNIQUE INDEX idx_hashtag_stats_hashtag ON hashtag_stats(hashtag);
CREATE INDEX idx_hashtag_stats_total_posts ON hashtag_stats(total_posts);

-- Aspect statistics (updated periodically)
CREATE MATERIALIZED VIEW aspect_stats AS
SELECT 
    aspect,
    COUNT(*) as mention_count,
    AVG(sentiment) as avg_sentiment,
    STDDEV(sentiment) as sentiment_stddev,
    COUNT(DISTINCT hashtag) as hashtag_count,
    COUNT(DISTINCT user_name) as user_count,
    MAX(processing_time) as last_seen,
    MIN(processing_time) as first_seen
FROM aspect_sentiments
GROUP BY aspect
HAVING COUNT(*) >= 3;  -- Only include aspects mentioned at least 3 times

CREATE UNIQUE INDEX idx_aspect_stats_aspect ON aspect_stats(aspect);
CREATE INDEX idx_aspect_stats_mention_count ON aspect_stats(mention_count);

-- ============================================================================
-- VIEWS (real-time, not materialized)
-- ============================================================================

-- Hashtag-aspect relationships with sentiment
CREATE OR REPLACE VIEW hashtag_aspect_sentiment AS
SELECT 
    a.hashtag,
    a.aspect,
    COUNT(*) as co_occurrence,
    AVG(a.sentiment) as avg_aspect_sentiment,
    STDDEV(a.sentiment) as sentiment_stddev,
    MAX(a.processing_time) as last_seen
FROM aspect_sentiments a
GROUP BY a.hashtag, a.aspect
HAVING COUNT(*) >= 2;

-- Recent posts summary (last 24 hours)
CREATE OR REPLACE VIEW recent_posts_summary AS
SELECT 
    DATE_TRUNC('hour', processing_time) as hour,
    COUNT(*) as post_count,
    AVG(sentiment) as avg_sentiment,
    COUNT(DISTINCT user_name) as unique_users,
    COUNT(DISTINCT hashtag) as unique_hashtags
FROM posts
WHERE processing_time > NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', processing_time)
ORDER BY hour DESC;

-- Trending aspects (last hour)
CREATE OR REPLACE VIEW trending_aspects AS
SELECT 
    aspect,
    COUNT(*) as recent_mentions,
    AVG(sentiment) as avg_sentiment,
    COUNT(DISTINCT hashtag) as hashtag_spread
FROM aspect_sentiments
WHERE processing_time > NOW() - INTERVAL '1 hour'
GROUP BY aspect
HAVING COUNT(*) >= 5
ORDER BY recent_mentions DESC;

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY hashtag_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY aspect_stats;
END;
$$ LANGUAGE plpgsql;

-- Function to clean old data (optional - for data retention)
CREATE OR REPLACE FUNCTION cleanup_old_data(days_to_keep INT)
RETURNS TABLE(table_name TEXT, rows_deleted BIGINT) AS $$
DECLARE
    deleted_posts BIGINT;
    deleted_aspects BIGINT;
    deleted_anomalies BIGINT;
BEGIN
    -- Delete old posts
    DELETE FROM posts WHERE processing_time < NOW() - (days_to_keep || ' days')::INTERVAL;
    GET DIAGNOSTICS deleted_posts = ROW_COUNT;
    
    -- Delete old aspect sentiments
    DELETE FROM aspect_sentiments WHERE processing_time < NOW() - (days_to_keep || ' days')::INTERVAL;
    GET DIAGNOSTICS deleted_aspects = ROW_COUNT;
    
    -- Delete old anomalies
    DELETE FROM anomaly_detection WHERE detected_at < NOW() - (days_to_keep || ' days')::INTERVAL;
    GET DIAGNOSTICS deleted_anomalies = ROW_COUNT;
    
    -- Return results
    RETURN QUERY SELECT 'posts'::TEXT, deleted_posts;
    RETURN QUERY SELECT 'aspect_sentiments'::TEXT, deleted_aspects;
    RETURN QUERY SELECT 'anomaly_detection'::TEXT, deleted_anomalies;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Trigger to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_hashtag_centrality_updated_at
    BEFORE UPDATE ON hashtag_centrality
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_clusters_updated_at
    BEFORE UPDATE ON user_clusters
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- INITIAL DATA / SETUP
-- ============================================================================

-- Create a sequence for batch IDs (if needed for monitoring)
CREATE SEQUENCE IF NOT EXISTS batch_id_seq START 1;

-- ============================================================================
-- GRANT PRIVILEGES
-- ============================================================================

-- Grant privileges to the mastodon user
GRANT ALL PRIVILEGES ON DATABASE mastodon_analytics TO mastodon;

-- Grant privileges on all tables
GRANT ALL PRIVILEGES ON TABLE posts TO mastodon;
GRANT ALL PRIVILEGES ON TABLE aspect_sentiments TO mastodon;
GRANT ALL PRIVILEGES ON TABLE hashtag_centrality TO mastodon;
GRANT ALL PRIVILEGES ON TABLE hashtag_communities TO mastodon;
GRANT ALL PRIVILEGES ON TABLE user_hashtag_network TO mastodon;
GRANT ALL PRIVILEGES ON TABLE user_clusters TO mastodon;
GRANT ALL PRIVILEGES ON TABLE trending_predictions TO mastodon;
GRANT ALL PRIVILEGES ON TABLE anomaly_detection TO mastodon;

-- Grant privileges on sequences
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO mastodon;

-- Grant privileges on materialized views
GRANT SELECT ON hashtag_stats TO mastodon;
GRANT SELECT ON aspect_stats TO mastodon;

-- Grant privileges on views
GRANT SELECT ON hashtag_aspect_sentiment TO mastodon;
GRANT SELECT ON recent_posts_summary TO mastodon;
GRANT SELECT ON trending_aspects TO mastodon;

-- Grant execute on functions
GRANT EXECUTE ON FUNCTION refresh_materialized_views() TO mastodon;
GRANT EXECUTE ON FUNCTION cleanup_old_data(INT) TO mastodon;

-- If you have additional users (e.g., for Grafana read-only access)
-- CREATE USER grafana_reader WITH PASSWORD 'your_grafana_password';
-- GRANT CONNECT ON DATABASE mastodon_analytics TO grafana_reader;
-- GRANT USAGE ON SCHEMA public TO grafana_reader;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO grafana_reader;

-- For future tables created in the schema
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO mastodon;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO mastodon;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Run these to verify the setup
-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;
-- SELECT viewname FROM pg_views WHERE schemaname = 'public';
-- SELECT matviewname FROM pg_matviews WHERE schemaname = 'public';

-- ============================================================================
-- MAINTENANCE NOTES
-- ============================================================================

-- Refresh materialized views periodically (e.g., every 5 minutes via cron):
-- */5 * * * * psql -U mastodon -d mastodon_analytics -c "SELECT refresh_materialized_views();"

-- Clean up old data (e.g., keep last 90 days):
-- SELECT * FROM cleanup_old_data(90);

-- Vacuum and analyze for performance:
-- VACUUM ANALYZE posts;
-- VACUUM ANALYZE aspect_sentiments;

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================