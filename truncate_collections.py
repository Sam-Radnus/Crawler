#!/usr/bin/env python3
"""
Utility script to truncate (delete all documents from) crawler MongoDB collections.

Usage:
  python truncate_collections.py --config ./config.json

This preserves indexes by using delete_many({}) instead of dropping collections.
"""
import argparse
import json
import sys
from typing import List

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


DEFAULT_COLLECTIONS = [
    "pages",
    "crawl_stats",
    "visited_pages",
]


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return json.load(f)


def truncate_collections(connection_string: str, database_name: str, collections: List[str]) -> None:
    client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
    # Validate connection
    client.admin.command("ping")
    db = client[database_name]

    for coll_name in collections:
        coll = db[coll_name]
        result = coll.delete_many({})
        print(f"Truncated {coll_name}: deleted {result.deleted_count} documents")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Truncate crawler MongoDB collections (preserve indexes)")
    parser.add_argument("--config", default="config.json", help="Path to configuration file")
    parser.add_argument(
        "--collections",
        nargs="*",
        default=DEFAULT_COLLECTIONS,
        help=f"Collections to truncate (default: {', '.join(DEFAULT_COLLECTIONS)})",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        cfg = load_config(args.config)
        db_cfg = cfg.get("database", {})
        connection_string = db_cfg.get("connection_string", "mongodb://localhost:27017/")
        database_name = db_cfg.get("database_name", "web_crawler")

        truncate_collections(connection_string, database_name, args.collections)
        print("Done.")
        return 0
    except FileNotFoundError:
        print(f"Config not found: {args.config}")
        return 1
    except ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")
        return 2
    except Exception as e:
        print(f"Error: {e}")
        return 3


if __name__ == "__main__":
    sys.exit(main())


