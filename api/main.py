from fastapi import FastAPI, Query, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor
import json

app = FastAPI()
config = {}
with open("config.json", "r") as f:
    config = json.load(f)

CONNECTION_STRING = config.get("database", {}).get("connection_string", None)

def get_connection():
    return psycopg2.connect(CONNECTION_STRING, cursor_factory=RealDictCursor)

@app.get("/")
def root():
    return {"message": "Welcome to the Web Crawler API"}

@app.get("/properties")
def get_properties(limit: int = Query(100, ge=1), offset: int = Query(0, ge=0)):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                    """
                       SELECT id, url, title, price, geohash, property_type, city, image_path 
                       FROM public.pages 
                       LIMIT %s OFFSET %s 
                    """, (limit, offset)
            )
            return cursor.fetchall()

# Place specific routes BEFORE parameterized routes to avoid conflicts
@app.get("/properties/nearby")
def get_nearby_properties(lat: float = Query(..., description="Latitude"), 
                          lon: float = Query(..., description="Longitude"), 
                          radius: int = Query(..., description="Radius in meters"),
                          limit: int = Query(1000, le=5000),
                          offset: int = Query(0, ge=0)):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT id, url, title, price, geohash, property_type, city, image_path 
                FROM public.pages WHERE ST_DWithin(geohash, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
                LIMIT {limit} OFFSET {offset}
            """, (lon, lat, radius))
            return cursor.fetchall()

@app.get("/properties/heatmap")
async def get_properties_heatmap(
        sw_lat: float = Query(None, ge=-90, le=90),
        sw_lng: float = Query(None, ge=-180, le=180),
        ne_lat: float = Query(None, ge=-90, le=90),
        ne_lng: float = Query(None, ge=-180, le=180),
        grid_size: float = Query(0.01, ge=0.001, le=1.0)
    ):
        
        query = f"""
            SELECT 
                ROUND(CAST(latitude AS numeric) / {grid_size}) * {grid_size} as latitude,
                ROUND(CAST(longitude AS numeric) / {grid_size}) * {grid_size} as longitude,
                COUNT(*) as weight
            FROM public.pages
            WHERE latitude BETWEEN {sw_lat} AND {ne_lat}
            AND longitude BETWEEN {sw_lng} AND {ne_lng}
            GROUP BY ROUND(CAST(latitude AS numeric) / {grid_size}), ROUND(CAST(longitude AS numeric) / {grid_size})
            ORDER BY weight DESC
            LIMIT 10000
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()

@app.get("/properties/bbox")
def get_properties_by_bbox(sw_lat: float = Query(..., ge=-90, le=90),
    sw_lng: float = Query(..., ge=-180, le=180),
    ne_lat: float = Query(..., ge=-90, le=90),
    ne_lng: float = Query(..., ge=-180, le=180),
    limit: int = Query(1000, le=5000)):
        
        query = f"""
            SELECT id, url, title, latitude, longitude, geohash, beds, baths, sqft, image_path
            FROM public.pages
            WHERE latitude BETWEEN {sw_lat} AND {ne_lat}
            AND longitude BETWEEN {sw_lng} AND {ne_lng}
            LIMIT {limit}
        """

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()


@app.get("/properties/city/{city}")
def get_properties_by_city(city: str, limit: int = Query(100, ge=1), offset: int = Query(0, ge=0)):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, url, title, price, geohash, property_type, city, image_path FROM public.pages WHERE city = %s LIMIT %s OFFSET %s", (city, limit, offset))
            return cursor.fetchall()

@app.get("/properties/{id}")
def get_property(id: int):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, url, title, price, geohash, property_type, city, image_path  FROM public.pages WHERE id = %s", (id,))
            result = cursor.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Property not found")
            return result
