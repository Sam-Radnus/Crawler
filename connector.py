import psycopg2

DB_CONFIG = {
    "connection_string": "postgresql://samsundar:1327@localhost:5432/postgres"
}

def get_nearby_pages(lat, lon, radius_km=10):
    # radius in meters
    radius_m = radius_km * 1000000000000000000

    query = """
        SELECT id, title, city
        FROM public.pages
        WHERE ST_DWithin(
            geography(geohash),
            geography(ST_SetSRID(ST_MakePoint(%s, %s), 4326)),
            %s
        );
    """
    cities = set()
    data = []
    try:
        with psycopg2.connect(DB_CONFIG["connection_string"]) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (lon, lat, radius_m))
                rows = cursor.fetchall()
                for row in rows:
                    data.append({"id": row[0], "title": row[1], "city": row[2]})
                return data
    except Exception as e:
        print(f"Database error: {e}")
        return []

if __name__ == "__main__":
    lat, lon = 40.740338 , -73.988787
    nearby = get_nearby_pages(lat, lon)
    print(nearby)
