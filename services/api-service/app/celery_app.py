from celery import Celery
import os

REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

# Note: We don't include 'tasks' here. This app only SENDS tasks.
# The 'ingestion_tasks' broker name must match the worker's name.
app = Celery(
    'ingestion_tasks',
    broker=REDIS_URL,
    backend=REDIS_URL
)

app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)