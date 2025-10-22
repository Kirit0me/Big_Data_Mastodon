#!/bin/bash
# Enhanced startup script for the full pipeline

KAFKA_DIR=$HOME/kafka
SPARK_DIR=$HOME/spark
PROJECT_DIR=/mnt/mint_bloat/projects/big_data_mastadon/with_ml
PYTHON_VENV=/mnt/mint_bloat/projects/big_data_mastadon/venv
KAFKA_BOOTSTRAP="localhost:9092"
SPARK_VERSION="3.5.0"

echo "🚀 Starting Full Mastodon Analytics Pipeline"
echo "=========================================="

# Cleanup
echo "Cleaning up old processes..."
pkill -f kafka
pkill -f zookeeper
pkill -f spark
pkill -f producer
pkill -f graph_analytics
pkill -f ml_training
sleep 2
rm -rf /tmp/zookeeper /tmp/kafka-logs /tmp/spark-checkpoints*

# Start Zookeeper & Kafka
echo "Starting Zookeeper & Kafka..."
gnome-terminal -- bash -c "$KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties; exec bash"
sleep 5
gnome-terminal -- bash -c "$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties; exec bash"
sleep 5

# Create topics
echo "Creating Kafka topics..."
$KAFKA_DIR/bin/kafka-topics.sh --create --if-not-exists --topic mastodon-raw \
  --bootstrap-server $KAFKA_BOOTSTRAP --partitions 1 --replication-factor 1
$KAFKA_DIR/bin/kafka-topics.sh --create --if-not-exists --topic mastodon-processed \
  --bootstrap-server $KAFKA_BOOTSTRAP --partitions 1 --replication-factor 1

# Start All Services in new terminals
echo "Starting Python services..."
gnome-terminal -- bash -c "cd $PROJECT_DIR && source $PYTHON_VENV/bin/activate && python3 producer_updated.py; exec bash"
gnome-terminal -- bash -c "cd $PROJECT_DIR && source $PYTHON_VENV/bin/activate && python3 graph_analytics.py; exec bash"
gnome-terminal -- bash -c "cd $PROJECT_DIR && source $PYTHON_VENV/bin/activate && python3 ml_training.py; exec bash"

# Start the Enhanced Spark Consumer
gnome-terminal -- bash -c "cd $PROJECT_DIR && source $PYTHON_VENV/bin/activate && \
  $SPARK_DIR/bin/spark-submit --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:$SPARK_VERSION \
  --driver-memory 2g consumer_ml_enhanced.py; exec bash"

echo ''
echo '✅ Full pipeline started!'
echo '  - Mastodon Producer'
echo '  - Spark Consumer (with ML features)'
echo '  - Graph Analytics Service'
echo '  - ML Training Service'
echo ''
echo '🔗 Configure Grafana at http://localhost:3000'