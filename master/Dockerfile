FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install Python dependencies (minimal for master)
COPY master/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only necessary files for master
COPY master/src/crawler/core/master.py ./src/crawler/core/
COPY master/src/crawler/prioritizer.py ./src/crawler/
COPY master/src/crawler/__init__.py ./src/crawler/
COPY master/src/__init__.py ./src/
COPY config.json ./config.json
COPY master/main.py .

# Set Python path
ENV PYTHONPATH=/app

CMD ["python", "-m", "src.crawler.core.master"]
