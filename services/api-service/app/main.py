from fastapi import FastAPI, HTTPException, status, Depends
from pydantic import BaseModel
from typing import List, Optional
from .celery_app import app as celery_client
from . import db  # Import our new db connection manager
import datetime

app = FastAPI(title="PulseIQ API Service")

# --- Pydantic Models ---
class TopicRequest(BaseModel):
    topic_name: str

class TaskStatus(BaseModel):
    task_id: str
    status: str
    message: str

# --- API Endpoints ---

@app.get("/", summary="Health check")
def read_root():
    return {"status": "ok", "message": "PulseIQ API Service is running."}


@app.post("/api/topic/start", 
          response_model=TaskStatus, 
          status_code=status.HTTP_202_ACCEPTED,
          summary="Trigger on-demand scraping for a new topic")
async def start_topic_scraping(request: TopicRequest):
    """
    This endpoint fulfills the 'On-Demand User Search'.
    It sends a task to the ingestion worker.
    """
    try:
        task = celery_client.send_task(
            'start_user_topic_collection', # Name of the task
            args=[request.topic_name]      # Arguments
        )
        return TaskStatus(
            task_id=task.id,
            status="PENDING",
            message=f"Task to scrape topic '{request.topic_name}' has been submitted."
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit task: {e}"
        )

# =======================================================
# REAL ENDPOINTS (Replaced Placeholders)
# =======================================================

@app.get("/api/topic/{topic_name}", summary="Get time-series data for a topic")
async def get_topic_data(topic_name: str, time_range_hours: int = 24):
    """
    Queries TimescaleDB for aggregated sentiment data over time.
    """
    conn = None
    try:
        conn = db.get_timescale_conn()
        if conn is None:
            raise HTTPException(status_code=503, detail="Database connection failed.")
            
        cur = conn.cursor()
        
        # This query groups data into 1-hour buckets for a clean chart
        query = """
        SELECT 
            time_bucket(INTERVAL '1 hour', time) AS hour,
            sentiment_label,
            SUM(sentiment_count) AS total_count
        FROM topic_sentiment_rollups
        WHERE 
            topic = %s 
            AND time > NOW() - INTERVAL '%s hours'
        GROUP BY hour, sentiment_label
        ORDER BY hour ASC;
        """
        
        cur.execute(query, (topic_name, time_range_hours))
        results = cur.fetchall()
        
        return {
            "topic": topic_name,
            "time_range_hours": time_range_hours,
            "data": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")
    finally:
        if conn:
            conn.close()


@app.get("/api/search", summary="Full-text search for posts")
async def search_posts(q: str, limit: int = 10):
    """
    Queries Elasticsearch for full-text search.
    """
    if not db.es_client:
        raise HTTPException(status_code=503, detail="Search service connection failed.")
        
    try:
        # Use Elasticsearch's "multi_match" to search the 'text' and 'topic' fields
        search_body = {
            "query": {
                "multi_match": {
                    "query": q,
                    "fields": ["text", "topic"]
                }
            },
            "size": limit,
            "_source": ["topic", "text", "source", "url", "timestamp"] # Only return these fields
        }
        
        response = db.es_client.search(
            index="pulseiq_posts",
            body=search_body
        )
        
        # Format the hits nicely
        hits = [hit['_source'] for hit in response['hits']['hits']]
        return {
            "query": q,
            "hit_count": response['hits']['total']['value'],
            "hits": hits
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search query error: {e}")


@app.get("/api/trending", summary="Get currently trending topics")
async def get_trending_topics():
    if not db.redis_client:
        raise HTTPException(status_code=503, detail="Cache service connection failed.")
        
    try:
        topics = list(db.redis_client.smembers("hot_topics") or [])
        return {"trending_topics": topics}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache query error: {e}")


# (We will build the /map endpoint after adding geo-data to our scrapers)