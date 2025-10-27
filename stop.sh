#!/bin/bash
set -euo pipefail

RESET_DATA=false

# Parse arguments
while getopts "d" opt; do
  case $opt in
    d)
      RESET_DATA=true
      ;;
    *)
      echo "Usage: $0 [-d]"
      exit 1
      ;;
  esac
done

echo "🛑 Stopping Web Crawler..."
docker-compose down

if [ "$RESET_DATA" = true ]; then
  echo "🧹 Removing Kafka/Zookeeper data volumes..."
  docker volume rm zookeeper-data || true
  docker volume rm kafka1-data || true
  docker volume rm kafka2-data || true
  docker volume rm kafka3-data || true
  echo "✅ Kafka/Zookeeper data reset successfully!"
fi

echo "✅ All services stopped!"
