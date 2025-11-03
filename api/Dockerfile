FROM python:3.9-slim

WORKDIR /app

RUN pip install fastapi psycopg2-binary uvicorn  

COPY master/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/main.py ./main.py
COPY config.json ./config.json

# add geospatial service
COPY geospatial/ ./geospatial/
COPY geospatial/prioritizer.py ./geospatial/prioritizer.py
COPY geospatial/state_coords.json ./geospatial/state_coords.json
COPY geospatial/generate_coords.py ./geospatial/generate_coords.py

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]