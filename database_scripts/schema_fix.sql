-- First, let's check what columns the posts table actually has
-- Run this first to see the structure:
-- \d posts

-- Fix existing posts table by adding missing columns if they don't exist
DO $$ 
BEGIN
    -- Add user_name column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name='posts' AND column_name='user_name') THEN
        -- Check if 'user' column exists instead
        IF EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name='posts' AND column_name='user') THEN
            ALTER TABLE posts RENAME COLUMN "user" TO user_name;
        ELSE
            ALTER TABLE posts ADD COLUMN user_name VARCHAR(255);
        END IF;
    END IF;

    -- Add sentiment column if missing
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name='posts' AND column_name='sentiment') THEN
        ALTER TABLE posts ADD COLUMN sentiment FLOAT;
    END IF;

    -- Add content_length column if missing
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name='posts' AND column_name='content_length') THEN
        ALTER TABLE posts ADD COLUMN content_length FLOAT;
    END IF;

    -- Add hashtag_count column if missing
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name='posts' AND column_name='hashtag_count') THEN
        ALTER TABLE posts ADD COLUMN hashtag_count INT;
    END IF;

    -- Add processing_time column if missing
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name='posts' AND column_name='processing_time') THEN
        ALTER TABLE posts ADD COLUMN processing_time TIMESTAMP;
    END IF;
END $$;

-- Create indexes if they don't exist
CREATE INDEX IF NOT EXISTS idx_posts_processing_time ON posts(processing_time);
CREATE INDEX IF NOT EXISTS idx_posts_hashtag ON posts(hashtag);
CREATE INDEX IF NOT EXISTS idx_posts_user ON posts(user_name);

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

-- Drop existing materialized view if it exists (to recreate with correct columns)
DROP MATERIALIZED VIEW IF EXISTS hashtag_stats;

-- Materialized view for fast hashtag stats
CREATE MATERIALIZED VIEW hashtag_stats AS
SELECT 
    hashtag,
    COUNT(*) as total_posts,
    AVG(sentiment) as avg_sentiment,
    COUNT(DISTINCT user_name) as unique_users,
    MAX(processing_time) as last_seen
FROM posts
WHERE user_name IS NOT NULL  -- Only include rows with user_name
GROUP BY hashtag;

CREATE INDEX idx_hashtag_stats_hashtag ON hashtag_stats(hashtag);

-- Grant permissions to mastodon user
GRANT ALL ON SCHEMA public TO mastodon;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO mastodon;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO mastodon;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO mastodon;

-- Refresh the materialized view
REFRESH MATERIALIZED VIEW hashtag_stats;