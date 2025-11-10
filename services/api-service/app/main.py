from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import List
from .celery_app import app as celery_client
import datetime

app = FastAPI(title="PulseIQ API Service")

# --- Pydantic Models ---
class TopicRequest(BaseModel):
    topic_name: str

class TaskStatus(BaseModel):
    task_id: str
    status: str
    message: str

# --- Endpoints ---
@app.get("/", summary="Health check")
def read_root():
    return {"status": "ok", "message": "PulseIQ API Service is running."}

@app.post("/api/topic/start", 
          response_model=TaskStatus, 
          status_code=status.HTTP_202_ACCEPTED,
          summary="Trigger on-demand scraping for a new topic")
async def start_topic_scraping(request: TopicRequest):
    """
    [cite_start]This endpoint fulfills the 'On-Demand User Search'[cite: 29].
    It sends a task to the ingestion worker.
    """
    try:
        # Send the task to our Celery worker
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

# --- Placeholder Endpoints (to be built in Phase 3B) ---

@app.get("/api/topic/{topic_name}", summary="Get time-series data for a topic")
async def get_topic_data(topic_name: str, time_range: str = "7d"):
    """
    (Placeholder) This will query TimescaleDB.
    """
    return {
        "topic": topic_name,
        "time_range": time_range,
        "message": "(Placeholder) Time-series data from TimescaleDB will go here.",
        "data": [
            {"time": "2025-11-10T14:00:00Z", "sentiment": "joy", "count": 150},
            {"time": "2025-11-10T14:01:00Z", "sentiment": "anger", "count": 30},
        ]
    }

@app.get("/api/topic/{topic_name}/map", summary="Get geographic heatmap data")
async def get_topic_map_data(topic_name: str):
    """
    (Placeholder) This will query TimescaleDB for geo-rollups.
    """
    return {
        "topic": topic_name,
        "message": "(Placeholder) Geo-heatmap data from TimescaleDB will go here.",
        "data": {
            "US": {"dominant_emotion": "joy", "score": 0.8},
            "IN": {"dominant_emotion": "surprise", "score": 0.6},
            "GB": {"dominant_emotion": "anger", "score": 0.5},
        }
    }

@app.get("/api/search", summary="Full-text search for posts")
async def search_posts(q: str):
    """
    (Placeholder) This will query Elasticsearch.
    """
    return {
        "query": q,
        "message": "(Placeholder) Full-text search results from Elasticsearch will go here.",
        "hits": [
            {"source": "reddit", "text": "This is a great post about {q}"},
            {"source": "twitter", "text": "I can't believe {q} is happening!"},
        ]
    }

@app.get("/api/trending", summary="Get currently trending topics")
async def get_trending_topics():
    """
    (Placeholder) This will query Redis.
    """
    return {
        "message": "(Placeholder) Trending topics from Redis will go here.",
        "topics": ["Tesla", "Apple", "NVIDIA", "Reuters"]
    }