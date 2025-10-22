-- Main posts table
CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY,
    user_name VARCHAR(255),
    content TEXT,
    hashtag VARCHAR(255),
    sentiment FLOAT,
    content_length FLOAT,
    hashtag_count INT,
    processing_time TIMESTAMP,
    timestamp TIMESTAMP
);

CREATE INDEX idx_posts_processing_time ON posts(processing_time);
CREATE INDEX idx_posts_hashtag ON posts(hashtag);
CREATE INDEX idx_posts_user ON posts(user_name);

-- Hashtag centrality metrics
CREATE TABLE IF NOT EXISTS hashtag_centrality (
    hashtag VARCHAR(255) PRIMARY KEY,
    degree_centrality FLOAT,
    betweenness_centrality FLOAT,
    pagerank FLOAT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Community detection results
CREATE TABLE IF NOT EXISTS hashtag_communities (
    hashtag VARCHAR(255),
    community_id INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (hashtag, community_id)
);

-- User network
CREATE TABLE IF NOT EXISTS user_hashtag_network (
    user_name VARCHAR(255),
    hashtag VARCHAR(255),
    co_occurrence_count INT DEFAULT 1,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_name, hashtag)
);

-- User clusters
CREATE TABLE IF NOT EXISTS user_clusters (
    user_name VARCHAR(255) PRIMARY KEY,
    cluster_id INT,
    avg_sentiment FLOAT,
    avg_content_length FLOAT,
    post_count INT,
    unique_hashtags INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trending predictions
CREATE TABLE IF NOT EXISTS trending_predictions (
    hashtag VARCHAR(255),
    prediction_score FLOAT,
    current_velocity FLOAT,
    predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (hashtag, predicted_at)
);

-- Anomaly detection
CREATE TABLE IF NOT EXISTS anomaly_detection (
    id SERIAL PRIMARY KEY,
    user_name VARCHAR(255),
    hashtag VARCHAR(255),
    anomaly_score FLOAT,
    reason TEXT,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Materialized view for fast hashtag stats
CREATE MATERIALIZED VIEW IF NOT EXISTS hashtag_stats AS
SELECT 
    hashtag,
    COUNT(*) as total_posts,
    AVG(sentiment) as avg_sentiment,
    COUNT(DISTINCT user_name) as unique_users,
    MAX(processing_time) as last_seen
FROM posts
GROUP BY hashtag;

CREATE INDEX idx_hashtag_stats_hashtag ON hashtag_stats(hashtag);

-- Refresh function (call periodically)
-- REFRESH MATERIALIZED VIEW CONCURRENTLY hashtag_stats;