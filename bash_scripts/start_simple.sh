#!/bin/bash
# Simple startup script focusing on getting data to Grafana

KAFKA_DIR=$HOME/kafka
SPARK_DIR=$HOME/spark
PROJECT_DIR=/mnt/mint_bloat/projects/big_data_mastadon/simple
PYTHON_VENV=/mnt/mint_bloat/projects/big_data_mastadon/venv
KAFKA_BOOTSTRAP="localhost:9092"
SPARK_VERSION="3.5.0"

echo "🚀 Starting Mastodon → Grafana Pipeline"
echo "========================================"

# Cleanup
echo "Cleaning up old processes..."
pkill -f kafka
pkill -f zookeeper
pkill -f spark
sleep 2
rm -rf /tmp/zookeeper /tmp/kafka-logs
rm -rf /tmp/spark-checkpoints*

# Start Zookeeper
echo "Starting Zookeeper..."
gnome-terminal -- bash -c "$KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties; exec bash"
sleep 5

# Start Kafka
echo "Starting Kafka..."
gnome-terminal -- bash -c "$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties; exec bash"
sleep 5

# Create topics
echo "Creating Kafka topics..."
$KAFKA_DIR/bin/kafka-topics.sh --create --if-not-exists --topic mastodon-raw \
  --bootstrap-server $KAFKA_BOOTSTRAP --partitions 1 --replication-factor 1

$KAFKA_DIR/bin/kafka-topics.sh --create --if-not-exists --topic mastodon-processed \
  --bootstrap-server $KAFKA_BOOTSTRAP --partitions 1 --replication-factor 1

# Start Producer
echo "Starting Mastodon Producer..."
gnome-terminal -- bash -c "cd $PROJECT_DIR && source $PYTHON_VENV/bin/activate && python3 producer_updated.py; exec bash"

# Start Spark Consumer 
echo "Starting Spark Consumer..."
gnome-terminal -- bash -c "cd $PROJECT_DIR && source $PYTHON_VENV/bin/activate && \
  $SPARK_DIR/bin/spark-submit --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:$SPARK_VERSION \
  --driver-memory 2g consumer_simple.py; exec bash"

echo ""
echo "✅ Pipeline started!"
echo ""
echo "Services running:"
echo "  ✓ Zookeeper"
echo "  ✓ Kafka"
echo "  ✓ Mastodon Producer → mastodon-raw"
echo "  ✓ Spark Consumer → PostgreSQL + mastodon-processed"
echo ""
echo "Monitor data flow:"
echo "  watch -n 2 'psql -h localhost -U mastodon -d mastodon_analytics -c \"SELECT COUNT(*) FROM posts;\"'"
echo ""
echo "Configure Grafana:"
echo "  http://localhost:3000"