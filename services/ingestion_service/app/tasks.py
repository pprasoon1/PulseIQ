from datetime import datetime, timezone
from .celery_app import app
from .scrapers.reddit import RedditScraper
from .scrapers.gnews import GNewsScraper
from .scrapers.twitter import TwitterScraper
from .scrapers.yc import YCScraper
import pymongo
import redis
import os
import requests
import json
import psycopg2
from psycopg2.extras import execute_values
from elasticsearch import Elasticsearch, helpers
from bson.objectid import ObjectId

# --- Database & Cache Setup ---
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
NLP_SERVICE_URL = "http://nlp-service:8001/process"
TIMESCALE_DB_URL = os.environ.get(
    'TIMESCALE_DB_URL', 'postgresql://postgres:postgres@localhost:5432/pulseiq_api_db')
ELASTICSEARCH_URL = os.environ.get(
    'ELASTICSEARCH_URL', 'http://elasticsearch:9200')

# ----- MongoDB -----
try:
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client['pulseiq_db']
    raw_posts_collection = db['raw_posts']
    analyzed_posts_collection = db['analyzed_posts']
    print("MongoDB connection successful.")
except Exception as e:
    print(f"Failed to connect to Mongo: {e}")

# ----- Redis -----
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    print("Redis connection successful.")
except Exception as e:
    print(f"Failed to connect to Redis: {e}")

# ----- TimescaleDB -----
try:
    ts_conn = psycopg2.connect(TIMESCALE_DB_URL)
    ts_conn.close()
    print("TimescaleDB connection successful.")
except Exception as e:
    print(f"Failed to connect to TimescaleDB: {e}")

# ----- Elasticsearch (Proper ES 8.x init) -----
try:
    es_client = Elasticsearch(
        ELASTICSEARCH_URL,
        verify_certs=False,
        request_timeout=30
    )
    info = es_client.info()
    print("Elasticsearch connection successful.")
except Exception as e:
    print(f"Failed to connect to Elasticsearch: {e}")


# -----------------------------------------------
# Tier Definitions
# -----------------------------------------------
TIER_1_TOPICS = {
    "Reliance Industries", "Jio", "Tata Group", "Infosys", "Wipro",
    "HDFC Bank", "Adani Group", "Zomato", "Swiggy", "Paytm", "Byju’s",
    "Ola Electric", "Ather Energy", "Airtel", "LIC", "ISRO", "Nykaa",
    "Hindustan Unilever", "Flipkart", "MakeMyTrip", "Delhi", "Mumbai", "Bengaluru"
}
TIER_2_REDIS_KEY = "hot_topics"
TIER_3_REDIS_KEY = "priority_topics"

reddit_scraper = RedditScraper()
gnews_scraper = GNewsScraper()
twitter_scraper = TwitterScraper()
yc_scraper = YCScraper()


# ========================================
# DISCOVERY TASKS
# ========================================
@app.task(name='discover_and_manage_topics')
def discover_and_manage_topics():
    print("--- Running discover_and_manage_topics ---")
    try:
        hot_topics = gnews_scraper.get_top_topics()
        if hot_topics:
            pipe = redis_client.pipeline()
            for topic in hot_topics:
                pipe.sadd(TIER_2_REDIS_KEY, topic)
                pipe.set(f"topic:{topic}", "tier2", ex=24 * 3600)
            pipe.execute()
            print(f"Added {len(hot_topics)} new topics to Tier 2")
    except Exception as e:
        print(f"Error in discover_and_manage_topics: {e}")


@app.task(name='collect_hot_topics')
def collect_hot_topics():
    print("--- Running collect_hot_topics ---")
    tier2_topics = redis_client.smembers(TIER_2_REDIS_KEY)
    tier3_topics = redis_client.smembers(TIER_3_REDIS_KEY)
    all_active = TIER_1_TOPICS.union(tier2_topics).union(tier3_topics)
    print(f"Total active topics to scrape: {len(all_active)}")
    for topic in all_active:
        collect_data_for_topic.delay(topic)


# ========================================
# USER TRIGGERED
# ========================================
@app.task(name='start_user_topic_collection')
def start_user_topic_collection(topic: str):
    print(f"User request for topic: {topic}")
    try:
        pipe = redis_client.pipeline()
        pipe.sadd(TIER_3_REDIS_KEY, topic)
        pipe.set(f"topic:{topic}", "tier3", ex=72 * 3600)
        pipe.execute()
        print(f"Added '{topic}' to Tier 3")
        collect_data_for_topic.delay(topic)
    except Exception as e:
        print(f"Error in start_user_topic_collection: {e}")


# ========================================
# WORKHORSE 1 — SCRAPING
# ========================================
@app.task(name='collect_data_for_topic')
def collect_data_for_topic(topic: str):
    print(f"Collecting data for: {topic}")
    all_posts = []

    scrapers = [
        ("Reddit", reddit_scraper),
        ("GNews", gnews_scraper),
        ("Twitter", twitter_scraper),
        ("Hacker News", yc_scraper)
    ]

    for name, scraper in scrapers:
        try:
            posts = scraper.scrape_topic(topic)
            if posts:
                all_posts.extend(posts)
                print(f"Collected {len(posts)} posts from {name} for '{topic}'")
        except Exception as e:
            print(f"Error scraping {name}: {e}")

    if not all_posts:
        return f"No new posts found for topic: {topic}"

    try:
        operations = [
            pymongo.UpdateOne(
                {'source_id': post['source_id']},
                {
                    '$set': post,
                    '$setOnInsert': {
                        'first_seen': datetime.now(timezone.utc),
                        'nlp_processed': False
                    }
                },
                upsert=True
            )
            for post in all_posts if post.get('source_id')
        ]

        if operations:
            result = raw_posts_collection.bulk_write(operations)
            print(
                f"Saved to 'raw_posts': {result.upserted_count} new, {result.modified_count} updated posts for '{topic}'")
        else:
            print(f"No posts with source_id found for topic '{topic}'")
    except Exception as e:
        print(f"Error saving posts to MongoDB: {e}")

    return f"Successfully collected {len(all_posts)} posts for: {topic}"


# ========================================
# WORKHORSE 2 — NLP PROCESSING
# ========================================
@app.task(name='process_raw_posts')
def process_raw_posts(batch_size: int = 25):
    print("--- Running process_raw_posts ---")

    posts = list(raw_posts_collection.find(
        {"nlp_processed": False}).limit(batch_size))

    if not posts:
        print("No new posts to process.")
        return "No new posts."

    print(f"Found {len(posts)} posts to process.")

    payload = [{"post_id": str(p['_id']), "text": p.get('text', '')} for p in posts]

    try:
        res = requests.post(NLP_SERVICE_URL, json=payload, timeout=60)
        res.raise_for_status()
        nlp_results = res.json()
    except Exception as e:
        print(f"Error calling NLP service: {e}")
        return "NLP service call failed."

    results_map = {res['post_id']: res for res in nlp_results}
    analyzed_ops = []
    ids = []

    for post in posts:
        pid = str(post['_id'])
        result = results_map.get(pid)
        if result:
            new_doc = post.copy()
            new_doc['sentiment'] = result['sentiment']
            new_doc['emotion'] = result['emotion']
            new_doc.pop('nlp_processed', None)
            new_doc['timescale_aggregated'] = False
            new_doc['es_indexed'] = False

            analyzed_ops.append(
                pymongo.UpdateOne({'_id': post['_id']}, {'$set': new_doc}, upsert=True)
            )
            ids.append(post['_id'])

    if analyzed_ops:
        analyzed_posts_collection.bulk_write(analyzed_ops)
        raw_posts_collection.update_many(
            {'_id': {'$in': ids}}, {'$set': {'nlp_processed': True}})
        print(f"Processed and saved {len(analyzed_ops)} posts.")

    return f"Successfully processed {len(nlp_results)} posts."


# ========================================
# TIMESCALE AGGREGATION
# ========================================
@app.task(name='aggregate_to_timescale')
def aggregate_to_timescale(batch_size: int = 1000):
    print("--- Running aggregate_to_timescale ---")

    posts = list(analyzed_posts_collection.find(
        {"timescale_aggregated": False}).limit(batch_size))

    if not posts:
        print("No new posts to aggregate.")
        return "No new posts."

    rollups = {}
    ids = []

    for post in posts:
        ids.append(post['_id'])
        if 'timestamp' not in post or not isinstance(post['timestamp'], datetime):
            continue

        bucket = post['timestamp'].replace(second=0, microsecond=0)
        topic = post['topic']
        label = post['sentiment']['label'].upper()

        key = (bucket, topic, label)
        rollups[key] = rollups.get(key, 0) + 1

    analyzed_posts_collection.update_many(
        {'_id': {'$in': ids}},
        {'$set': {'timescale_aggregated': True}}
    )

    if not rollups:
        print("No valid rollups.")
        return "No valid rollups."

    data = [(k[0], k[1], k[2], c) for k, c in rollups.items()]

    query = """
    INSERT INTO topic_sentiment_rollups (time, topic, sentiment_label, sentiment_count)
    VALUES %s
    ON CONFLICT (time, topic, sentiment_label)
    DO UPDATE SET sentiment_count = topic_sentiment_rollups.sentiment_count + EXCLUDED.sentiment_count;
    """

    try:
        conn = psycopg2.connect(TIMESCALE_DB_URL)
        cur = conn.cursor()
        execute_values(cur, query, data)
        conn.commit()
        cur.close()
        conn.close()
        print(f"Upserted {len(data)} rollups into TimescaleDB.")
    except Exception as e:
        print(f"Error inserting into TimescaleDB: {e}")

    return f"Aggregated {len(data)} rollups."


# ========================================
# ELASTICSEARCH INDEXING  ✅ FIXED
# ========================================
@app.task(name='index_to_elasticsearch')
def index_to_elasticsearch(batch_size: int = 1000):
    print("--- Running index_to_elasticsearch ---")

    try:
        es_client.indices.create(index="pulseiq_posts", ignore=[400])
        print("Ensured Elasticsearch index exists.")
    except Exception as e:
        print(f"Error creating Elasticsearch index: {e}")
        return

    posts = list(analyzed_posts_collection.find(
        {"es_indexed": False}).limit(batch_size))

    if not posts:
        print("No new posts to index.")
        return

    actions = []
    ids = []

    for post in posts:
        ids.append(post['_id'])
        doc = post.copy()
        doc_id = str(doc.pop('_id'))

        if 'timestamp' in doc and isinstance(doc['timestamp'], datetime):
            doc['timestamp'] = doc['timestamp'].isoformat()
        if 'first_seen' in doc and isinstance(doc['first_seen'], datetime):
            doc['first_seen'] = doc['first_seen'].isoformat()

        actions.append({
            "_op_type": "index",
            "_index": "pulseiq_posts",
            "_id": doc_id,
            "_source": doc
        })

    try:
        helpers.bulk(es_client, actions)
        print(f"Indexed {len(actions)} documents.")

        analyzed_posts_collection.update_many(
            {'_id': {'$in': ids}}, {'$set': {'es_indexed': True}})
        print(f"Marked {len(ids)} posts as indexed.")

    except Exception as e:
        print(f"Error during bulk index: {e}")

    return f"Attempted to index {len(actions)} posts."
