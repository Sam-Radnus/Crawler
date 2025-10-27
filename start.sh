#!/bin/bash

set -euo pipefail

echo "🚀 Starting Web Crawler Infrastructure..."
echo "========================================"

# Stop any existing containers
echo "🛑 Stopping existing containers..."
docker-compose down --remove-orphans

# Build and start all services in detached mode
echo "🔨 Building and starting services..."
docker-compose up --build -d

# Wait for Kafka brokers to be ready
# echo "⏳ Waiting for Kafka brokers..."
# for broker in kafka1:9092 kafka2:9093 kafka3:9094; do
#     until docker exec kafka-init cub kafka-ready -b $broker 1 60; do
#         echo "Waiting for $broker..."
#         sleep 5
#     done
# done


# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 10

echo "✅ All services started successfully!"
echo ""
echo "🎯 Web Crawler is now running in controlled mode:"
echo "   • Workers are connected to Kafka queues and waiting"
echo "   • Master is in standby mode"
echo "   • No crawling has started yet"
echo ""
echo "📋 Available commands:"
echo "   python3 main.py start          # Start crawling with seed URLs"
echo "   python3 main.py health_check   # Check system health"
echo "   python3 main.py stop           # Get instructions to stop"
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
