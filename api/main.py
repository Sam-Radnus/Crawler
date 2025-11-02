from fastapi import FastAPI, Query, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from contextlib import contextmanager

app = FastAPI()

# Load configuration
try:
    with open("config.json", "r") as f:
        config = json.load(f)
    CONNECTION_STRING = config.get("database", {}).get("connection_string")
    if not CONNECTION_STRING:
        raise ValueError("Database connection string not found in config.json")
except FileNotFoundError:
    raise RuntimeError("config.json file not found")
except json.JSONDecodeError:
    raise RuntimeError("config.json is not valid JSON")

@contextmanager
def get_connection():
    conn = None
    try:
        conn = psycopg2.connect(CONNECTION_STRING, cursor_factory=RealDictCursor)
        yield conn
        conn.commit()
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if conn:
            conn.close()

@app.get("/")
def root():
    return {"message": "Welcome to the Web Crawler API"}

@app.get("/properties")
def get_properties(
    limit: int = Query(100, ge=1, le=5000), 
    offset: int = Query(0, ge=0)
):
    query = """
        SELECT id, url, title, price, geohash, property_type, city, image_path 
        FROM public.pages 
        LIMIT %s OFFSET %s
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (limit, offset))
            return cursor.fetchall()

@app.get("/properties/nearby")
def get_nearby_properties(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    radius: int = Query(..., gt=0, description="Radius in meters"),
    limit: int = Query(1000, ge=1, le=5000),
    offset: int = Query(0, ge=0)
):
    query = """
        SELECT id, url, title, price, geohash, property_type, city, image_path 
        FROM public.pages 
        WHERE ST_DWithin(geohash, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
        LIMIT %s OFFSET %s
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (lon, lat, radius, limit, offset))
            return cursor.fetchall()

@app.get("/properties/similar/nearby")
def get_similar_properties_nearby(
    min_price: int = Query(..., ge=0),
    max_price: int = Query(..., ge=0),
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radius: int = Query(..., gt=0),
    property_type: str = Query(None),
    limit: int = Query(1000, ge=1, le=5000),
    offset: int = Query(0, ge=0)
):
    if min_price > max_price:
        raise HTTPException(status_code=400, detail="min_price cannot exceed max_price")
    
    property_types = property_type.split(',') if property_type else None
    
    if property_types:
        query = """
            SELECT id, url, title, price, geohash, property_type, city, image_path 
            FROM public.pages 
            WHERE ST_DWithin(geohash, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
            AND price BETWEEN %s AND %s
            AND property_type = ANY(%s)
            LIMIT %s OFFSET %s
        """
        params = (lon, lat, radius, min_price, max_price, property_types, limit, offset)
    else:
        query = """
            SELECT id, url, title, price, geohash, property_type, city, image_path 
            FROM public.pages 
            WHERE ST_DWithin(geohash, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
            AND price BETWEEN %s AND %s
            LIMIT %s OFFSET %s
        """
        params = (lon, lat, radius, min_price, max_price, limit, offset)
    
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

@app.get("/properties/similar/{property_id}")
def get_similar_properties(
    property_id: int,
    price_diff: int = Query(..., ge=0),
    radius: int = Query(..., gt=0),
    limit: int = Query(1000, ge=1, le=5000),
    offset: int = Query(0, ge=0)
):
    query = """
        WITH base_property AS (
            SELECT price, geohash, property_type 
            FROM public.pages 
            WHERE id = %s
        )
        SELECT p.id, p.url, p.title, p.price, p.geohash, p.property_type, p.city, p.image_path
        FROM public.pages p, base_property bp
        WHERE p.id != %s
        AND ST_DWithin(p.geohash, bp.geohash, %s)
        AND p.price BETWEEN (bp.price - %s) AND (bp.price + %s)
        AND p.property_type = bp.property_type
        LIMIT %s OFFSET %s
    """
    
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (property_id, property_id, radius, price_diff, price_diff, limit, offset))
            results = cursor.fetchall()
            
            if not results:
                cursor.execute("SELECT id FROM public.pages WHERE id = %s", (property_id,))
                if not cursor.fetchone():
                    raise HTTPException(status_code=404, detail="Property not found")
            
            return results

@app.get("/properties/heatmap")
def get_properties_heatmap(
    sw_lat: float = Query(..., ge=-90, le=90),
    sw_lng: float = Query(..., ge=-180, le=180),
    ne_lat: float = Query(..., ge=-90, le=90),
    ne_lng: float = Query(..., ge=-180, le=180),
    grid_size: float = Query(0.01, ge=0.001, le=1.0)
):
    if sw_lat >= ne_lat or sw_lng >= ne_lng:
        raise HTTPException(status_code=400, detail="Invalid bounding box coordinates")
    
    query = """
        SELECT 
            ROUND(CAST(latitude AS numeric) / %s) * %s as latitude,
            ROUND(CAST(longitude AS numeric) / %s) * %s as longitude,
            COUNT(*) as weight
        FROM public.pages
        WHERE latitude BETWEEN %s AND %s
        AND longitude BETWEEN %s AND %s
        GROUP BY ROUND(CAST(latitude AS numeric) / %s), ROUND(CAST(longitude AS numeric) / %s)
        ORDER BY weight DESC
        LIMIT 10000
    """
    
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (
                grid_size, grid_size, grid_size, grid_size,
                sw_lat, ne_lat, sw_lng, ne_lng,
                grid_size, grid_size
            ))
            return cursor.fetchall()

@app.get("/properties/bbox")
def get_properties_by_bbox(
    sw_lat: float = Query(..., ge=-90, le=90),
    sw_lng: float = Query(..., ge=-180, le=180),
    ne_lat: float = Query(..., ge=-90, le=90),
    ne_lng: float = Query(..., ge=-180, le=180),
    limit: int = Query(1000, ge=1, le=5000)
):
    if sw_lat >= ne_lat or sw_lng >= ne_lng:
        raise HTTPException(status_code=400, detail="Invalid bounding box coordinates")
    
    query = """
        SELECT id, url, title, latitude, longitude, geohash, beds, baths, sqft, image_path
        FROM public.pages
        WHERE latitude BETWEEN %s AND %s
        AND longitude BETWEEN %s AND %s
        LIMIT %s
    """
    
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (sw_lat, ne_lat, sw_lng, ne_lng, limit))
            return cursor.fetchall()

@app.get("/properties/city/{city}")
def get_properties_by_city(
    city: str,
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0)
):
    query = """
        SELECT id, url, title, price, geohash, property_type, city, image_path 
        FROM public.pages 
        WHERE city = %s 
        LIMIT %s OFFSET %s
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (city, limit, offset))
            return cursor.fetchall()

@app.get("/properties/{id}")
def get_property(id: int):
    query = """
        SELECT id, url, title, price, geohash, property_type, city, image_path  
        FROM public.pages 
        WHERE id = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (id,))
            result = cursor.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Property not found")
            return result