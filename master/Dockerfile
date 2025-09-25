FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install Python dependencies (minimal for master)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only necessary files for master
COPY src/crawler/core/master.py ./src/crawler/core/
COPY src/crawler/prioritizer.py ./src/crawler/
COPY src/crawler/__init__.py ./src/crawler/
COPY src/__init__.py ./src/
COPY config.json .

# Set Python path
ENV PYTHONPATH=/app

CMD ["python", "-m", "src.crawler.core.master"]
