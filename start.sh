#!/bin/bash

set -euo pipefail

echo "🚀 Starting Web Crawler with Docker Compose..."
echo "=============================================="

# Create necessary directories
mkdir -p crawled_data

# Stop any existing containers
echo "🛑 Stopping existing containers..."
docker-compose down --remove-orphans

# Build and start all services
echo "🔨 Building and starting services..."
docker-compose up --build

echo "✅ All services started successfully!"
echo ""
echo "📊 To view logs from all services:"
echo "   docker-compose logs -f"
echo ""
echo "📊 To view logs from specific service:"
echo "   docker-compose logs -f <service-name>"
echo "   Available services: master, worker-1, worker-2, worker-3, worker-4, worker-5, kafka, mongodb"
echo ""
echo "🛑 To stop all services:"
echo "   docker-compose down"
echo ""
echo "🗑️  To stop and remove all data:"
echo "   docker-compose down -v"
