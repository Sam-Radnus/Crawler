#!/bin/bash

set -euo pipefail

# Default to showing all logs if no service specified
SERVICE=${1:-""}

echo "📊 Web Crawler Logs"
echo "=================="

if [ -z "$SERVICE" ]; then
    echo "Showing logs from all services..."
    echo "Use: ./logs.sh <service-name> to view specific service logs"
    echo ""
    echo "Available services:"
    echo "  - master"
    echo "  - worker-1, worker-2, worker-3, worker-4, worker-5"
    echo "  - kafka"
    echo "  - mongodb"
    echo ""
    docker-compose logs -f
else
    echo "Showing logs from service: $SERVICE"
    docker-compose logs -f "$SERVICE"
fi
