import logging
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.extras import RealDictCursor


class DatabaseService:
    """PostgreSQL-backed storage for crawled pages.

    Minimal interface required by worker:
      - get_page(url) -> Optional[dict]
      - save_page(url, html_content, status_code, headers, crawl_duration) -> dict
      - close() -> None
    """

    def __init__(
        self,
        connection_string: str,
        database_name: str = "postgres",
        output_dir: str = "crawled_data",
    ) -> None:
        self.connection_string = connection_string
        self.database_name = database_name
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__)
        self._conn = None

        self._connect()
        self._ensure_schema()

    def _connect(self) -> None:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                self.connection_string,
                cursor_factory=RealDictCursor)
            self._conn.autocommit = True

    def _ensure_schema(self) -> None:
        """Create the pages table if it doesn't exist.

        The schema is a superset so API queries selecting optional columns still work
        (they will be NULL if not populated by the worker).
        """
        create_sql = """
        CREATE TABLE IF NOT EXISTS public.pages (
            id SERIAL PRIMARY KEY,
            url TEXT UNIQUE NOT NULL,
            title TEXT,
            price NUMERIC,
            property_type TEXT,
            city TEXT,
            image_path TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            geohash GEOGRAPHY(POINT),
            beds INTEGER,
            baths INTEGER,
            sqft INTEGER,
            status_code INTEGER,
            headers JSONB,
            crawl_duration DOUBLE PRECISION,
            html_content TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
        # Enable PostGIS if available; ignore errors if extension
        # exists/missing perms
        enable_postgis = "CREATE EXTENSION IF NOT EXISTS postgis;"
        with self._conn.cursor() as cur:
            try:
                cur.execute(enable_postgis)
            except Exception:
                # Not fatal for insertion/existence checks
                pass
            cur.execute(create_sql)

    def get_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Return page row if exists; used by worker to dedupe."""
        self._connect()
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT id, url FROM public.pages WHERE url = %s LIMIT 1", (url,))
            row = cur.fetchone()
            return row if row else None

    def save_page(
        self,
        url: str,
        html_content: str,
        storage_path: str,
        status_code: int,
        crawl_duration: float,
    ) -> Dict[str, Any]:
        """Upsert a page record. Returns minimal info for logging."""
        self._connect()
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.pages (url, status_code, crawl_duration, storage_path)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE SET
                  status_code = EXCLUDED.status_code,
                  crawl_duration = EXCLUDED.crawl_duration
                RETURNING id, url;
                """,
                (url, status_code, crawl_duration, storage_path),
            )
            return cur.fetchone() or {"url": url}

    def close(self) -> None:
        try:
            if self._conn and not self._conn.closed:
                self._conn.close()
        except Exception:
            pass
