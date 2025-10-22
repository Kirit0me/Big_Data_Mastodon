#!/bin/bash
# Deployment script for Mastodon Analytics on Kubernetes

set -e

PROJECT_DIR="/mnt/mint_bloat/projects/big_data_mastadon"
REGISTRY="your-docker-registry"  # e.g., docker.io/yourusername or gcr.io/your-project
VERSION="v1.0"

echo "🚀 Starting deployment..."

# ========================================
# STEP 1: Build Docker Images
# ========================================
echo "📦 Building Docker images..."

cd $PROJECT_DIR

# Producer
docker build -f Dockerfile.producer -t ${REGISTRY}/mastodon-producer:${VERSION} .
docker tag ${REGISTRY}/mastodon-producer:${VERSION} ${REGISTRY}/mastodon-producer:latest

# Spark Consumer
docker build -f Dockerfile.spark -t ${REGISTRY}/mastodon-spark:${VERSION} .
docker tag ${REGISTRY}/mastodon-spark:${VERSION} ${REGISTRY}/mastodon-spark:latest

# Analytics (Graph + ML)
docker build -f Dockerfile.analytics -t ${REGISTRY}/mastodon-analytics:${VERSION} .
docker tag ${REGISTRY}/mastodon-analytics:${VERSION} ${REGISTRY}/mastodon-analytics:latest

echo "✅ Docker images built"

# ========================================
# STEP 2: Push to Registry
# ========================================
echo "📤 Pushing images to registry..."

docker push ${REGISTRY}/mastodon-producer:${VERSION}
docker push ${REGISTRY}/mastodon-producer:latest

docker push ${REGISTRY}/mastodon-spark:${VERSION}
docker push ${REGISTRY}/mastodon-spark:latest

docker push ${REGISTRY}/mastodon-analytics:${VERSION}
docker push ${REGISTRY}/mastodon-analytics:latest

echo "✅ Images pushed"

# ========================================
# STEP 3: Update Kubernetes manifests
# ========================================
echo "🔧 Updating K8s manifests with registry..."

# Replace placeholder with actual registry
find k8s/ -name "*.yaml" -exec sed -i "s|your-registry|${REGISTRY}|g" {} \;

echo "✅ Manifests updated"

# ========================================
# STEP 4: Deploy to Kubernetes
# ========================================
echo "☸️  Deploying to Kubernetes..."

# Create namespace
kubectl apply -f k8s/00-namespace.yaml

# Apply configs and secrets
kubectl apply -f k8s/01-configmap.yaml

# Deploy infrastructure
kubectl apply -f k8s/02-zookeeper.yaml
kubectl apply -f k8s/03-kafka.yaml
kubectl apply -f k8s/04-postgres.yaml

echo "⏳ Waiting for infrastructure to be ready..."
kubectl wait --for=condition=ready pod -l app=zookeeper -n mastodon-analytics --timeout=300s
kubectl wait --for=condition=ready pod -l app=kafka -n mastodon-analytics --timeout=300s
kubectl wait --for=condition=ready pod -l app=postgres -n mastodon-analytics --timeout=300s

# Initialize database schema
echo "🗄️  Initializing database schema..."
kubectl exec -n mastodon-analytics postgres-0 -- psql -U mastodon -d mastodon_analytics -f /docker-entrypoint-initdb.d/init.sql || true

# Deploy application components
kubectl apply -f k8s/05-producer.yaml
kubectl apply -f k8s/06-spark-consumer.yaml
kubectl apply -f k8s/07-analytics.yaml
kubectl apply -f k8s/08-grafana.yaml

echo "✅ Deployment complete!"

# ========================================
# STEP 5: Display status
# ========================================
echo ""
echo "📊 Deployment Status:"
echo "===================="
kubectl get pods -n mastodon-analytics
echo ""
echo "🌐 Services:"
kubectl get svc -n mastodon-analytics
echo ""
echo "📈 Access Grafana:"
echo "   kubectl port-forward -n mastodon-analytics svc/grafana-service 3000:3000"
echo "   Then open: http://localhost:3000 (admin/admin)"
echo ""
echo "🔍 View logs:"
echo "   kubectl logs -n mastodon-analytics -l app=producer -f"
echo "   kubectl logs -n mastodon-analytics -l app=spark-consumer -f"
echo "   kubectl logs -n mastodon-analytics -l app=graph-analytics -f"
echo ""
echo "🎉 Done!"