from fastapi import FastAPI, HTTPException, status, Depends, Query
from pydantic import BaseModel
from typing import List, Optional, Dict
from .celery_app import app as celery_client
from . import db
import datetime

app = FastAPI(title="PulseIQ API Service")

# --------------------------------------------------------
# Pydantic Models
# --------------------------------------------------------
class TopicRequest(BaseModel):
    topic_name: str

class TaskStatus(BaseModel):
    task_id: str
    status: str
    message: str


# --------------------------------------------------------
# Health Check
# --------------------------------------------------------
@app.get("/", summary="Health check")
def read_root():
    return {"status": "ok", "message": "PulseIQ API Service is running."}


# --------------------------------------------------------
# TOPIC SCRAPING (USER TRIGGER)
# --------------------------------------------------------
@app.post(
    "/api/topic/start",
    response_model=TaskStatus,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger on-demand scraping for a new topic"
)
async def start_topic_scraping(request: TopicRequest):
    try:
        task = celery_client.send_task(
            "start_user_topic_collection",
            args=[request.topic_name]
        )
        return TaskStatus(
            task_id=task.id,
            status="PENDING",
            message=f"Task to scrape topic '{request.topic_name}' has been submitted."
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit task: {e}"
        )


# --------------------------------------------------------
# SEARCH ENDPOINT
# --------------------------------------------------------
@app.get("/api/search", summary="Full-text search for posts")
async def search_posts(q: str, limit: int = 10):
    if not db.es_client:
        raise HTTPException(status_code=503, detail="Search service connection failed.")
    try:
        search_body = {
            "query": {
                "multi_match": {
                    "query": q,
                    "fields": ["text", "topic"]
                }
            },
            "size": limit,
            "_source": ["topic", "text", "source", "url", "timestamp"]
        }

        response = db.es_client.search(index="pulseiq_posts", body=search_body)
        hits = [hit["_source"] for hit in response["hits"]["hits"]]

        return {
            "query": q,
            "hit_count": response["hits"]["total"]["value"],
            "hits": hits
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search query error: {e}")


# --------------------------------------------------------
# TRENDING TOPICS
# --------------------------------------------------------
@app.get("/api/trending", summary="Get currently trending topics")
async def get_trending_topics():
    if not db.redis_client:
        raise HTTPException(status_code=503, detail="Cache service connection failed.")
    try:
        topics = db.redis_client.smembers("hot_topics")
        return {"trending_topics": list(topics)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache query error: {e}")


# ===================================================================
# VERY IMPORTANT: These endpoints must come BEFORE /api/topic/{topic}
# ===================================================================

# --------------------------------------------------------
# TOPIC COMPARISON (NEW)
# --------------------------------------------------------
@app.get("/api/topic/compare", summary="Compare time-series data for multiple topics")
async def get_topic_comparison(
    topic: List[str] = Query(..., min_length=2, max_length=5),
    time_range_hours: int = 24
):
    conn = None
    try:
        conn = db.get_timescale_conn()
        if conn is None:
            raise HTTPException(status_code=503, detail="Database connection failed.")
        
        cur = conn.cursor()

        query = """
        SELECT 
            time_bucket(INTERVAL '1 hour', time) AS hour,
            topic,
            sentiment_label,
            SUM(sentiment_count) AS total_count
        FROM topic_sentiment_rollups
        WHERE 
            topic = ANY(%s)
            AND time > NOW() - (INTERVAL '1 hour' * %s)
        GROUP BY hour, topic, sentiment_label
        ORDER BY hour ASC;
        """

        cur.execute(query, (topic, time_range_hours))
        results = cur.fetchall()

        comparison_data: Dict[str, List] = {t: [] for t in topic}
        for row in results:
            comparison_data[row["topic"]].append(row)

        return {
            "topics": topic,
            "time_range_hours": time_range_hours,
            "data": comparison_data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")

    finally:
        if conn:
            conn.close()


# --------------------------------------------------------
# TOPIC HEATMAP (GEO)
# --------------------------------------------------------
@app.get("/api/topic/{topic_name}/map", summary="Get geographic heatmap data")
async def get_topic_map_data(topic_name: str, time_range_hours: int = 24):
    conn = None
    try:
        conn = db.get_timescale_conn()
        if conn is None:
            raise HTTPException(status_code=503, detail="Database connection failed.")
        
        cur = conn.cursor()

        query = """
        WITH CountrySentiments AS (
            SELECT
                country_code,
                sentiment_label,
                SUM(sentiment_count) AS total_count
            FROM topic_sentiment_rollups
            WHERE
                topic = %s
                AND time > NOW() - (INTERVAL '1 hour' * %s)
                AND country_code != 'XX'
            GROUP BY country_code, sentiment_label
        ),
        RankedSentiments AS (
            SELECT
                country_code,
                sentiment_label,
                total_count,
                ROW_NUMBER() OVER(PARTITION BY country_code ORDER BY total_count DESC) AS rn
            FROM CountrySentiments
        )
        SELECT
            country_code,
            sentiment_label AS dominant_sentiment,
            total_count
        FROM RankedSentiments
        WHERE rn = 1;
        """

        cur.execute(query, (topic_name, time_range_hours))
        results = cur.fetchall()

        heatmap_data = {
            row["country_code"]: row["dominant_sentiment"]
            for row in results
        }

        return {
            "topic": topic_name,
            "time_range_hours": time_range_hours,
            "heatmap_data": heatmap_data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")

    finally:
        if conn:
            conn.close()


# --------------------------------------------------------
# TOPIC TIME-SERIES
# --------------------------------------------------------
@app.get("/api/topic/{topic_name}", summary="Get time-series data for a topic")
async def get_topic_data(topic_name: str, time_range_hours: int = 24):
    conn = None
    try:
        conn = db.get_timescale_conn()
        if conn is None:
            raise HTTPException(status_code=503, detail="Database connection failed.")
        
        cur = conn.cursor()

        query = """
        SELECT 
            time_bucket(INTERVAL '1 hour', time) AS hour,
            sentiment_label,
            SUM(sentiment_count) AS total_count
        FROM topic_sentiment_rollups
        WHERE 
            topic = %s 
            AND time > NOW() - (INTERVAL '1 hour' * %s)
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
