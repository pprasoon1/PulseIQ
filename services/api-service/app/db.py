import os
import psycopg2
import redis
from elasticsearch import Elasticsearch
from psycopg2.extras import RealDictCursor

# Load database URLs from environment variables
TIMESCALE_DB_URL = os.environ.get('TIMESCALE_DB_URL')
ELASTICSEARCH_URL = os.environ.get('ELASTICSEARCH_URL')
REDIS_URL = os.environ.get('REDIS_URL')

# --- Connection Pools ---
# We manage connections here so we don't open/close them on every request.

# 1. TimescaleDB (PostgreSQL)
# We don't create a pool, but a function to get a connection.
def get_timescale_conn():
    """
    Returns a new connection to the TimescaleDB.
    psycopg2's built-in pooling is complex; for this app,
    opening/closing a connection per request is robust.
    """
    try:
        conn = psycopg2.connect(TIMESCALE_DB_URL)
        # RealDictCursor returns results as dictionaries (like JSON)
        conn.cursor_factory = RealDictCursor
        return conn
    except Exception as e:
        print(f"Error connecting to TimescaleDB: {e}")
        return None

# 2. Elasticsearch
try:

    es_client = Elasticsearch("http://pulseiq-elasticsearch:9200", verify_certs=False)
    if not es_client.ping():
        raise Exception("Elasticsearch ping failed")
    print("API Service: Elasticsearch connection successful.")
except Exception as e:
    print(f"API Service: Failed to connect to Elasticsearch: {e}")
    es_client = None

# 3. Redis
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    if not redis_client.ping():
        raise Exception("Redis ping failed")
    print("API Service: Redis connection successful.")
except Exception as e:
    print(f"API Service: Failed to connect to Redis: {e}")
    redis_client = None