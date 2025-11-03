#!/bin/bash
set -e

echo "🔨 Building Docker images..."

# Build producer
echo "Building mastodon-producer..."
docker build -f Dockerfile.producer -t mastodon-producer:latest .

# Build Spark consumer
echo "Building spark-consumer..."
docker build -f Dockerfile.spark-consumer -t spark-consumer:latest .

# Build ML training
echo "Building ml-training..."
docker build -f Dockerfile.ml-training -t ml-training:latest .

# Build graph analytics
echo "Building graph-analytics..."
docker build -f Dockerfile.graph-analytics -t graph-analytics:latest .

echo "✅ All images built successfully!"
docker images | grep -E "(mastodon-producer|spark-consumer|ml-training|graph-analytics)"