from celery import Celery
from celery.schedules import crontab
import os

redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

app = Celery(
    'ingestion_tasks',
    broker=redis_url,
    backend=redis_url,
    include=['app.tasks']
)

# --- NEW: Define the autonomous schedule ---
app.conf.beat_schedule = {
    # The main autonomous engine, runs every 1 hour 
    'discover-manage-loop': {
        'task': 'discover_and_manage_topics',
        'schedule': crontab(minute='0', hour='*'), # Run at the top of every hour
    },
    # You can add more tasks, e.g., a "fast-loop" every 10 mins
    'fast-topic-loop': {
        'task': 'collect_hot_topics',
        'schedule': crontab(minute='*/10'), # Run every 10 minutes
    }
}

app.conf.update(
    task_serializer='json',
    accept_content=['json'],  
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)