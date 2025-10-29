FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install Python dependencies (minimal for master)
COPY master/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only necessary files for master
COPY master/src/crawler/core/master.py ./src/crawler/core/
COPY master/src/crawler/queue_manager.py ./src/crawler/
COPY master/src/crawler/robots_checker.py ./src/crawler/
COPY master/src/crawler/__init__.py ./src/crawler/
COPY master/main.py .

RUN mkdir -p ./src/crawler/utils
COPY worker/src/crawler/utils/property_matcher.py ./src/crawler/utils/
COPY worker/src/crawler/utils/__init__.py ./src/crawler/utils/
COPY master/src/__init__.py ./src/
# Copy geospatial module
COPY geospatial/ ./geospatial/
COPY config.json ./config.json
# Set Python path
ENV PYTHONPATH=/app
