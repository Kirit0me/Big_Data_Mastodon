#!/bin/bash
set -e

echo "🚀 Deploying to Kubernetes..."

# Create namespace
kubectl create namespace mastodon-analytics --dry-run=client -o yaml | kubectl apply -f -

# Create PostgreSQL init ConfigMap
echo "Creating PostgreSQL init script..."
kubectl create configmap postgres-init-sql \
  --from-file=schema_updated.sql \
  --namespace=mastodon-analytics \
  --dry-run=client -o yaml | kubectl apply -f -

# Apply Kubernetes manifests
echo "Applying Kubernetes manifests..."
kubectl apply -f k8s-deployment.yaml

# Wait for PostgreSQL
echo "Waiting for PostgreSQL to be ready..."
kubectl wait --for=condition=ready pod -l app=postgres \
  -n mastodon-analytics --timeout=300s || true

# Wait for Kafka
echo "Waiting for Kafka to be ready..."
sleep 30  # Give Kafka time to start

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📊 Check status:"
echo "  kubectl get pods -n mastodon-analytics"
echo ""
echo "📝 View logs:"
echo "  kubectl logs -f -l app=mastodon-producer -n mastodon-analytics"