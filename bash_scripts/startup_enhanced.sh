#!/bin/bash
# Enhanced startup script for the full pipeline
# Now with option to choose between basic and aspect-based sentiment

KAFKA_DIR=$HOME/kafka
SPARK_DIR=$HOME/spark
PROJECT_DIR=/mnt/mint_bloat/projects/big_data_mastadon/with_ml
PYTHON_VENV=/mnt/mint_bloat/projects/big_data_mastadon/venv
KAFKA_BOOTSTRAP="localhost:9092"
SPARK_VERSION="3.5.0"

# Color codes for pretty output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}🚀 Mastodon Analytics Pipeline Startup${NC}"
echo -e "${BLUE}========================================${NC}"

# Ask user which consumer to use
echo ""
echo -e "${YELLOW}Select Spark Consumer Type:${NC}"
echo "  1) Basic Sentiment (consumer_ml_enhanced.py)"
echo "     - Simple sentiment scores (-1 to +1)"
echo "     - Faster processing"
echo ""
echo "  2) Aspect-Based Sentiment (consumer_aspect_sentiment.py) [RECOMMENDED]"
echo "     - Detailed aspect extraction"
echo "     - Sentiment for each topic/entity"
echo "     - Sentiment categories"
echo ""
read -p "Enter choice (1 or 2): " consumer_choice

if [ "$consumer_choice" == "2" ]; then
    CONSUMER_SCRIPT="consumer_aspect_sentiment.py"
    CHECKPOINT_DIR="/tmp/spark-checkpoints-aspect"
    echo -e "${GREEN}✓ Using Aspect-Based Sentiment Consumer${NC}"
elif [ "$consumer_choice" == "1" ]; then
    CONSUMER_SCRIPT="consumer_ml_enhanced.py"
    CHECKPOINT_DIR="/tmp/spark-checkpoints"
    echo -e "${GREEN}✓ Using Basic Sentiment Consumer${NC}"
else
    echo -e "${RED}Invalid choice. Defaulting to Aspect-Based Consumer.${NC}"
    CONSUMER_SCRIPT="consumer_aspect_sentiment.py"
    CHECKPOINT_DIR="/tmp/spark-checkpoints-aspect"
fi

echo ""
echo -e "${YELLOW}Starting cleanup...${NC}"

# Cleanup old processes
echo "Stopping old processes..."
pkill -f kafka
pkill -f zookeeper
pkill -f spark
pkill -f producer
pkill -f graph_analytics
pkill -f ml_training
sleep 2

# Cleanup old data directories
echo "Cleaning up old data directories..."
rm -rf /tmp/zookeeper /tmp/kafka-logs /tmp/spark-checkpoints*

echo -e "${GREEN}✓ Cleanup complete${NC}"
echo ""

# Start Zookeeper
echo -e "${YELLOW}Starting Zookeeper...${NC}"
gnome-terminal -- bash -c "cd $PROJECT_DIR && echo -e '${BLUE}=== ZOOKEEPER ===${NC}' && $KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties; exec bash" 2>/dev/null
sleep 5
echo -e "${GREEN}✓ Zookeeper started${NC}"

# Start Kafka
echo -e "${YELLOW}Starting Kafka...${NC}"
gnome-terminal -- bash -c "cd $PROJECT_DIR && echo -e '${BLUE}=== KAFKA BROKER ===${NC}' && $KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties; exec bash" 2>/dev/null
sleep 5
echo -e "${GREEN}✓ Kafka started${NC}"

# Create Kafka topics
echo -e "${YELLOW}Creating Kafka topics...${NC}"
$KAFKA_DIR/bin/kafka-topics.sh --create --if-not-exists --topic mastodon-raw \
  --bootstrap-server $KAFKA_BOOTSTRAP --partitions 1 --replication-factor 1 2>/dev/null
$KAFKA_DIR/bin/kafka-topics.sh --create --if-not-exists --topic mastodon-processed \
  --bootstrap-server $KAFKA_BOOTSTRAP --partitions 1 --replication-factor 1 2>/dev/null
echo -e "${GREEN}✓ Topics created${NC}"

# Verify topics exist
echo -e "${YELLOW}Verifying topics...${NC}"
$KAFKA_DIR/bin/kafka-topics.sh --list --bootstrap-server $KAFKA_BOOTSTRAP
echo ""

# Start Mastodon Producer
echo -e "${YELLOW}Starting Mastodon Producer...${NC}"
gnome-terminal -- bash -c "cd $PROJECT_DIR && source $PYTHON_VENV/bin/activate && echo -e '${BLUE}=== MASTODON PRODUCER ===${NC}' && python3 producer_updated.py; exec bash" 2>/dev/null
sleep 2
echo -e "${GREEN}✓ Producer started${NC}"

# Start Graph Analytics Service
echo -e "${YELLOW}Starting Graph Analytics Service...${NC}"
gnome-terminal -- bash -c "cd $PROJECT_DIR && source $PYTHON_VENV/bin/activate && echo -e '${BLUE}=== GRAPH ANALYTICS ===${NC}' && python3 graph_analytics.py; exec bash" 2>/dev/null
sleep 1
echo -e "${GREEN}✓ Graph Analytics started${NC}"

# Start ML Training Service
echo -e "${YELLOW}Starting ML Training Service...${NC}"
gnome-terminal -- bash -c "cd $PROJECT_DIR && source $PYTHON_VENV/bin/activate && echo -e '${BLUE}=== ML TRAINING ===${NC}' && python3 ml_training.py; exec bash" 2>/dev/null
sleep 1
echo -e "${GREEN}✓ ML Training started${NC}"

# Start Spark Consumer (with selected script)
echo -e "${YELLOW}Starting Spark Consumer ($CONSUMER_SCRIPT)...${NC}"
gnome-terminal -- bash -c "cd $PROJECT_DIR && source $PYTHON_VENV/bin/activate && echo -e '${BLUE}=== SPARK CONSUMER ($CONSUMER_SCRIPT) ===${NC}' && \
  $SPARK_DIR/bin/spark-submit --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:$SPARK_VERSION \
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=4 \
  $CONSUMER_SCRIPT; exec bash" 2>/dev/null
sleep 2
echo -e "${GREEN}✓ Spark Consumer started${NC}"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✅ Full Pipeline Started Successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${BLUE}Running Components:${NC}"
echo "  ✓ Zookeeper"
echo "  ✓ Kafka Broker"
echo "  ✓ Mastodon Producer"
echo "  ✓ Spark Consumer ($CONSUMER_SCRIPT)"
echo "  ✓ Graph Analytics Service"
echo "  ✓ ML Training Service"
echo ""
echo -e "${BLUE}Next Steps:${NC}"
echo "  1. Monitor terminals for data flow"
echo "  2. Check database: psql -U mastodon -d mastodon_analytics"
if [ "$CONSUMER_SCRIPT" == "consumer_aspect_sentiment.py" ]; then
    echo "  3. Wait 30 mins, then check: SELECT COUNT(*) FROM aspect_sentiments;"
fi
echo "  4. Configure Grafana at http://localhost:3000"
echo ""
echo -e "${BLUE}Useful Commands:${NC}"
echo "  - View Kafka topics: $KAFKA_DIR/bin/kafka-topics.sh --list --bootstrap-server $KAFKA_BOOTSTRAP"
echo "  - Check posts: psql -U mastodon -d mastodon_analytics -c 'SELECT COUNT(*) FROM posts;'"
if [ "$CONSUMER_SCRIPT" == "consumer_aspect_sentiment.py" ]; then
    echo "  - Check aspects: psql -U mastodon -d mastodon_analytics -c 'SELECT aspect, COUNT(*) FROM aspect_sentiments GROUP BY aspect ORDER BY COUNT(*) DESC LIMIT 10;'"
fi
echo "  - Stop all: pkill -f 'kafka|zookeeper|spark|producer|graph_analytics|ml_training'"
echo ""
echo -e "${YELLOW}Press Ctrl+C in each terminal to stop individual services${NC}"
echo -e "${GREEN}========================================${NC}"