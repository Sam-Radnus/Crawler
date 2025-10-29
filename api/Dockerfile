FROM python:3.9-slim

WORKDIR /app

RUN pip install fastapi psycopg2-binary uvicorn

COPY api/main.py ./main.py
COPY config.json ./config.json

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]