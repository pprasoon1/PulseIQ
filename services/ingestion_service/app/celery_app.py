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

app.conf.beat_schedule = {
    'discover-manage-loop': {
        'task': 'discover_and_manage_topics',
        'schedule': crontab(minute='0', hour='*'), 
    },
    'fast-topic-loop': {
        'task': 'collect_hot_topics',
        'schedule': crontab(minute='*/10'),
    },
    'process-raw-posts-loop': {
        'task': 'process_raw_posts',
        'schedule': crontab(minute='*/1'),
    },
    
    # --- NEW: AGGREGATOR TASKS (PHASE 3B) ---
    'aggregate-to-timescale-loop': {
        'task': 'aggregate_to_timescale',
        'schedule': crontab(minute='*/5'), # Run every 5 minutes
    },
    'index-to-elasticsearch-loop': {
        'task': 'index_to_elasticsearch',
        'schedule': crontab(minute='*/5'), # Run every 5 minutes
    },
    # --- NEW: CRISIS ALERT TASK (PHASE 3E) ---
    'analyze-for-crisis': {
        'task': 'analyze_for_crisis',
        'schedule': crontab(minute='*/15'), # Run every 15 minutes
    }
}

# ... (app.conf.update is unchanged) ...

app.conf.update(
    task_serializer='json',
    accept_content=['json'],  
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)