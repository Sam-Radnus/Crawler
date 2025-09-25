#!/bin/bash

set -euo pipefail

echo "🛑 Stopping Web Crawler..."
echo "========================="

# Stop all services
docker-compose down

echo "✅ All services stopped successfully!"
