#!/bin/bash
set -e

echo "🚀 Setting up Minikube..."

# Check if minikube is already running
if minikube status &> /dev/null; then
    echo "Minikube is already running"
else
    echo "Starting Minikube..."
    minikube start --cpus=4 --memory=8192 --disk-size=40g
fi

# Enable addons
echo "Enabling addons..."
minikube addons enable metrics-server
minikube addons enable dashboard

echo "✅ Minikube is ready!"
minikube status