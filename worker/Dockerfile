FROM python:3.9-slim

WORKDIR /app

# Install system dependencies for Playwright and PostgreSQL client
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    ca-certificates \
    wget \
    gnupg \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*



# Copy requirements and install dependencies
COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN playwright install chromium
RUN playwright install-deps chromium

# Copy source
COPY worker/src/ ./src/

# Copy geospatial module
COPY geospatial/ ./geospatial/
COPY config.json ./config.json


ENV PYTHONPATH=/app

CMD ["python", "-m", "src.crawler.core.worker", "urls_priority_1"]
