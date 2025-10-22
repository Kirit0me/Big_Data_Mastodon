#!/bin/bash
# Plain startup script for Mastodon → Kafka → Spark → Streamlit pipeline

# === CONFIG ===
KAFKA_DIR=$HOME/kafka
SPARK_DIR=$HOME/spark
PROJECT_DIR=/mnt/mint_bloat/projects/big_data_mastadon/legacy
PYTHON_VENV=/mnt/mint_bloat/projects/big_data_mastadon/venv
KAFKA_BOOTSTRAP="localhost:9092"

TOPIC_RAW="mastodon-raw"
TOPIC_PROCESSED="mastodon-processed"

# Match this to your Spark version (check: spark-submit --version)
SPARK_VERSION="3.5.0"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:$SPARK_VERSION"

# === CLEANUP ===
pkill -f kafka
pkill -f zookeeper
pkill -f spark
sleep 1
rm -rf /tmp/zookeeper /tmp/kafka-logs
rm -rf /tmp/spark-checkpoints

# === START ZOOKEEPER ===
gnome-terminal -- bash -c "$KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties; exec bash"
sleep 5

# === START KAFKA BROKER ===
gnome-terminal -- bash -c "$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties; exec bash"
sleep 5

# === CREATE TOPICS ===
$KAFKA_DIR/bin/kafka-topics.sh --create --if-not-exists --topic $TOPIC_RAW \
  --bootstrap-server $KAFKA_BOOTSTRAP --partitions 1 --replication-factor 1

$KAFKA_DIR/bin/kafka-topics.sh --create --if-not-exists --topic $TOPIC_PROCESSED \
  --bootstrap-server $KAFKA_BOOTSTRAP --partitions 1 --replication-factor 1

# === START PRODUCER ===
gnome-terminal -- bash -c "cd $PROJECT_DIR && source $PYTHON_VENV/bin/activate && python3 producer_new.py; exec bash"

# === START SPARK CONSUMER (with Kafka package) ===
gnome-terminal -- bash -c "cd $PROJECT_DIR && source $PYTHON_VENV/bin/activate && \
  $SPARK_DIR/bin/spark-submit --master local[*] --packages $KAFKA_PACKAGE consumer_new.py; exec bash"

# === START DASHBOARD ===
gnome-terminal -- bash -c "cd $PROJECT_DIR && source $PYTHON_VENV/bin/activate && streamlit run dash.py --server.port 8501; exec bash"

echo "✅ Pipeline started!"
echo "  - Mastodon producer (→ $TOPIC_RAW)"
echo "  - Spark consumer (→ $TOPIC_PROCESSED)"
echo "  - Dashboard: http://localhost:8501"
