-- Dashboard Panel Queries for Grafana

-- 1. Posts per minute (Time series)
SELECT 
    DATE_TRUNC('minute', processing_time) as time,
    COUNT(*) as posts_count
FROM posts
WHERE processing_time > NOW() - INTERVAL '1 hour'
GROUP BY time
ORDER BY time;

-- 2. Top 10 trending hashtags (Bar chart)
SELECT 
    hashtag,
    COUNT(*) as count
FROM posts
WHERE processing_time > NOW() - INTERVAL '1 hour'
GROUP BY hashtag
ORDER BY count DESC
LIMIT 10;

-- 3. Sentiment distribution (Gauge/Stat)
SELECT 
    AVG(sentiment) as avg_sentiment,
    STDDEV(sentiment) as sentiment_stddev,
    MIN(sentiment) as min_sentiment,
    MAX(sentiment) as max_sentiment
FROM posts
WHERE processing_time > NOW() - INTERVAL '1 hour';

-- 4. Sentiment over time (Time series)
SELECT 
    DATE_TRUNC('minute', processing_time) as time,
    AVG(sentiment) as avg_sentiment
FROM posts
WHERE processing_time > NOW() - INTERVAL '1 hour'
GROUP BY time
ORDER BY time;

-- 5. Top users by activity (Table)
SELECT 
    user_name,
    COUNT(*) as post_count,
    AVG(sentiment) as avg_sentiment,
    COUNT(DISTINCT hashtag) as unique_hashtags
FROM posts
WHERE processing_time > NOW() - INTERVAL '1 hour'
GROUP BY user_name
ORDER BY post_count DESC
LIMIT 20;

-- 6. Hashtag network centrality (Table)
SELECT 
    hashtag,
    ROUND(degree_centrality::numeric, 4) as degree,
    ROUND(betweenness_centrality::numeric, 4) as betweenness,
    ROUND(pagerank::numeric, 4) as pagerank
FROM hashtag_centrality
ORDER BY pagerank DESC
LIMIT 20;

-- 7. Community distribution (Pie chart)
SELECT 
    community_id,
    COUNT(*) as hashtag_count
FROM hashtag_communities
GROUP BY community_id
ORDER BY hashtag_count DESC;

-- 8. User clusters (Stat panels)
SELECT 
    cluster_id,
    COUNT(*) as user_count,
    AVG(avg_sentiment) as cluster_sentiment,
    AVG(post_count) as avg_posts
FROM user_clusters
GROUP BY cluster_id
ORDER BY cluster_id;

-- 9. Trending predictions (Bar gauge)
SELECT 
    hashtag,
    prediction_score
FROM trending_predictions
WHERE predicted_at = (SELECT MAX(predicted_at) FROM trending_predictions)
ORDER BY prediction_score DESC
LIMIT 15;

-- 10. Recent anomalies (Table)
SELECT 
    detected_at as time,
    user_name,
    ROUND(anomaly_score::numeric, 2) as score,
    reason
FROM anomaly_detection
WHERE detected_at > NOW() - INTERVAL '1 hour'
ORDER BY detected_at DESC
LIMIT 20;

-- 11. Hourly post volume comparison (Time series with multiple series)
SELECT 
    DATE_TRUNC('hour', processing_time) as time,
    CASE 
        WHEN sentiment > 0.1 THEN 'positive'
        WHEN sentiment < -0.1 THEN 'negative'
        ELSE 'neutral'
    END as sentiment_category,
    COUNT(*) as count
FROM posts
WHERE processing_time > NOW() - INTERVAL '24 hours'
GROUP BY time, sentiment_category
ORDER BY time;

-- 12. Hashtag velocity (for alerting)
SELECT 
    hashtag,
    current_velocity,
    prediction_score
FROM trending_predictions
WHERE predicted_at > NOW() - INTERVAL '15 minutes'
    AND prediction_score > 0.7
ORDER BY prediction_score DESC;