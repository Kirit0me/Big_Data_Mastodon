#!/bin/bash
# Check what data is actually in Kafka

KAFKA_DIR=$HOME/kafka

echo "🔍 Checking Kafka messages..."
echo "=============================="

echo ""
echo "Last 5 messages from mastodon-raw topic:"
echo "-----------------------------------------"
$KAFKA_DIR/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic mastodon-raw \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 5000 2>/dev/null

echo ""
echo "Checking database for NULL user_name:"
echo "--------------------------------------"
psql -h localhost -U mastodon -d mastodon_analytics -c \
  "SELECT COUNT(*) as null_users FROM posts WHERE user_name IS NULL;"

echo ""
echo "Checking database for non-NULL user_name:"
echo "------------------------------------------"
psql -h localhost -U mastodon -d mastodon_analytics -c \
  "SELECT COUNT(*) as with_users FROM posts WHERE user_name IS NOT NULL;"

echo ""
echo "Sample of actual data in database:"
echo "-----------------------------------"
psql -h localhost -U mastodon -d mastodon_analytics -c \
  "SELECT user_name, hashtag, LEFT(content, 50) as content_preview FROM posts LIMIT 5;"