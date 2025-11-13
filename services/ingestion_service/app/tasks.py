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

# ========================================
# DATABASE & CACHE SETUP
# ========================================

MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

NLP_SERVICE_URL = "http://nlp-service:8001/process"
NLP_GEO_SERVICE_URL = "http://nlp-service:8001/process_geo"   # <-- NEW IMPORTANT LINE

TIMESCALE_DB_URL = os.environ.get(
    'TIMESCALE_DB_URL', 'postgresql://postgres:postgres@localhost:5432/pulseiq_api_db')

ELASTICSEARCH_URL = os.environ.get(
    'ELASTICSEARCH_URL', 'http://elasticsearch:9200')

# ----- Mongo -----
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

# ----- Timescale -----
try:
    ts_conn = psycopg2.connect(TIMESCALE_DB_URL)
    ts_conn.close()
    print("TimescaleDB connection successful.")
except Exception as e:
    print(f"Failed to connect to TimescaleDB: {e}")

# ----- Elasticsearch -----
try:
    es_client = Elasticsearch(
        ELASTICSEARCH_URL,
        verify_certs=False,
        request_timeout=30
    )
    es_client.info()
    print("Elasticsearch connection successful.")
except Exception as e:
    print(f"Failed to connect to Elasticsearch: {e}")

# ========================================
# TOPIC TIERS & SCRAPERS
# ========================================

TIER_1_TOPICS = {
    "Reliance Industries", "Jio", "Tata Group", "Infosys", "Wipro",
    "HDFC Bank", "Adani Group", "Zomato", "Swiggy", "Paytm",
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
            print(f"Added {len(hot_topics)} topics to Tier 2")
    except Exception as e:
        print(f"Error in discover_and_manage_topics: {e}")


@app.task(name='collect_hot_topics')
def collect_hot_topics():
    print("--- Running collect_hot_topics ---")
    tier2_topics = redis_client.smembers(TIER_2_REDIS_KEY)
    tier3_topics = redis_client.smembers(TIER_3_REDIS_KEY)
    all_topics = TIER_1_TOPICS.union(tier2_topics).union(tier3_topics)

    print(f"Scraping {len(all_topics)} topics")
    for topic in all_topics:
        collect_data_for_topic.delay(topic)


@app.task(name='start_user_topic_collection')
def start_user_topic_collection(topic: str):
    print(f"User-triggered topic collection: {topic}")

    try:
        pipe = redis_client.pipeline()
        pipe.sadd(TIER_3_REDIS_KEY, topic)
        pipe.set(f"topic:{topic}", "tier3", ex=72 * 3600)
        pipe.execute()

        collect_data_for_topic.delay(topic)
    except Exception as e:
        print(f"Error in start_user_topic_collection: {e}")


# ========================================
# SCRAPING WORKER
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
                print(f"{len(posts)} from {name}")
        except Exception as e:
            print(f"Error scraping {name}: {e}")

    if not all_posts:
        return f"No posts for {topic}"

    try:
        ops = [
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

        if ops:
            res = raw_posts_collection.bulk_write(ops)
            print(f"{res.upserted_count} new, {res.modified_count} updated")

    except Exception as e:
        print(f"Error saving posts: {e}")

    return f"Collected {len(all_posts)} posts."


# ========================================
# NLP + GEO PROCESSING WORKER (UPDATED)
# ========================================

@app.task(name='process_raw_posts')
def process_raw_posts(batch_size: int = 25):
    print("--- Running process_raw_posts ---")

    posts = list(raw_posts_collection.find(
        {"nlp_processed": False}).limit(batch_size))

    if not posts:
        return "No posts."

    print(f"Found {len(posts)} posts")

    # ---------------------------
    # 1. NLP ANALYSIS
    # ---------------------------
    payload = [{"post_id": str(p['_id']), "text": p.get('text', '')} for p in posts]

    try:
        res = requests.post(NLP_SERVICE_URL, json=payload, timeout=60)
        res.raise_for_status()
        nlp_results = res.json()
    except Exception as e:
        print(f"NLP service error: {e}")
        return "NLP failed."

    results_map = {r['post_id']: r for r in nlp_results}

    # ---------------------------
    # 2. GEO ANALYSIS (NEW)
    # ---------------------------
    geo_payload = []
    for post in posts:
        loc = post.get("metadata", {}).get("user_location_string")
        if loc:
            geo_payload.append({
                "post_id": str(post["_id"]),
                "location_string": loc
            })

    geo_map = {}
    if geo_payload:
        print(f"Geocoding {len(geo_payload)} posts...")
        try:
            geo_res = requests.post(NLP_GEO_SERVICE_URL, json=geo_payload, timeout=60)
            geo_res.raise_for_status()
            geo_results = geo_res.json()
            geo_map = {r["post_id"]: r for r in geo_results}
        except Exception as e:
            print(f"Geo service error: {e}")

    # ---------------------------
    # 3. MERGE + SAVE
    # ---------------------------
    ops = []
    ids = []

    for post in posts:
        pid = str(post["_id"])
        nlp = results_map.get(pid)

        if not nlp:
            continue

        new_doc = post.copy()
        new_doc["sentiment"] = nlp["sentiment"]
        new_doc["emotion"] = nlp["emotion"]
        new_doc["timescale_aggregated"] = False
        new_doc["es_indexed"] = False
        new_doc.pop("nlp_processed", None)

        # Attach geo results
        geo = geo_map.get(pid)
        if geo:
            new_doc["geo"] = {
                "country_code": geo.get("country_code"),
                "city": geo.get("city"),
                "state": geo.get("state")
            }

        ops.append(
            pymongo.UpdateOne({"_id": post["_id"]}, {"$set": new_doc}, upsert=True)
        )
        ids.append(post["_id"])

    if ops:
        analyzed_posts_collection.bulk_write(ops)
        raw_posts_collection.update_many(
            {"_id": {"$in": ids}}, {"$set": {"nlp_processed": True}}
        )
        print(f"Saved {len(ops)} posts.")

    return f"Processed {len(results_map)} posts."



# ========================================
# TIMESCALE AGGREGATION (MODIFIED FOR GEO)
# ========================================
@app.task(name='aggregate_to_timescale')
def aggregate_to_timescale(batch_size: int = 1000):
    print("--- Running aggregate_to_timescale (with Geo) ---")

    posts = list(analyzed_posts_collection.find(
        {"timescale_aggregated": False}).limit(batch_size))

    if not posts:
        print("No new posts to aggregate.")
        return "No new posts."

    print(f"Found {len(posts)} posts to aggregate for TimescaleDB.")

    # { (time_bucket, topic, label, country_code): count }
    rollups = {}
    ids = []

    for post in posts:
        ids.append(post['_id'])
        try:
            if 'timestamp' not in post or not isinstance(post['timestamp'], datetime):
                continue

            bucket = post['timestamp'].replace(second=0, microsecond=0)
            topic = post['topic']
            label = post['sentiment']['label'].upper()
            
            # --- NEW: Get country_code, default to 'XX' if unknown ---
            country_code = post.get('geo', {}).get('country_code', 'XX')
            # Skip if country_code is None or empty
            if not country_code:
                country_code = 'XX' 
            # --------------------------------------------------------

            key = (bucket, topic, label, country_code)
            rollups[key] = rollups.get(key, 0) + 1
            
        except Exception as e:
            print(f"Warning: Skipping post {post['_id']} due to aggregation error: {e}")
            continue

    analyzed_posts_collection.update_many(
        {'_id': {'$in': ids}},
        {'$set': {'timescale_aggregated': True}}
    )

    if not rollups:
        print("No valid rollups.")
        return "No valid rollups."

    # Data is now (time, topic, label, country_code, count)
    data = [(k[0], k[1], k[2], k[3], c) for k, c in rollups.items()]

    query = """
    INSERT INTO topic_sentiment_rollups (time, topic, sentiment_label, country_code, sentiment_count)
    VALUES %s
    ON CONFLICT (time, topic, sentiment_label, country_code)
    DO UPDATE SET sentiment_count = topic_sentiment_rollups.sentiment_count + EXCLUDED.sentiment_count;
    """

    try:
        conn = psycopg2.connect(TIMESCALE_DB_URL)
        cur = conn.cursor()
        execute_values(cur, query, data)
        conn.commit()
        cur.close()
        conn.close()
        print(f"Upserted {len(data)} geo-rollups into TimescaleDB.")
    except Exception as e:
        print(f"Error inserting into TimescaleDB: {e}")

    return f"Aggregated {len(data)} rollups."


# ========================================
# ELASTICSEARCH INDEXING (UNCHANGED)
# ========================================

@app.task(name='index_to_elasticsearch')
def index_to_elasticsearch(batch_size: int = 1000):
    print("--- Running index_to_elasticsearch ---")

    try:
        es_client.indices.create(index="pulseiq_posts", ignore=[400])
    except Exception:
        pass

    posts = list(analyzed_posts_collection.find(
        {"es_indexed": False}).limit(batch_size))

    if not posts:
        return "No posts."

    actions = []
    ids = []

    for post in posts:
        ids.append(post["_id"])
        doc = post.copy()
        doc_id = str(doc.pop("_id"))

        if "timestamp" in doc and isinstance(doc["timestamp"], datetime):
            doc["timestamp"] = doc["timestamp"].isoformat()
        if "first_seen" in doc and isinstance(doc["first_seen"], datetime):
            doc["first_seen"] = doc["first_seen"].isoformat()

        actions.append({
            "_op_type": "index",
            "_index": "pulseiq_posts",
            "_id": doc_id,
            "_source": doc
        })

    try:
        helpers.bulk(es_client, actions)
        analyzed_posts_collection.update_many(
            {"_id": {"$in": ids}}, {"$set": {"es_indexed": True}}
        )
        print(f"Indexed {len(actions)} posts.")
    except Exception as e:
        print(f"ES error: {e}")

    return f"Indexed {len(actions)} posts."



# ========================================
# NEW: CRISIS ALERT TASK (PHASE 3E)
# ========================================
@app.task(name='analyze_for_crisis')
def analyze_for_crisis():
    """
    Analyzes Tier 1 topics for sudden spikes in negative sentiment.
    """
    print("--- Running analyze_for_crisis ---")
    
    # Define what we consider "negative"
    NEGATIVE_LABELS = ('NEGATIVE', 'ANGER', 'FEAR', 'SADNESS')
    # Define our alert threshold (e.g., 2.0 = 100% increase)
    ALERT_THRESHOLD = 2.0 

    # This query gets negative counts for the last hour vs. the last 24 hours
    # for all Tier 1 topics in one go.
    query = """
    WITH stats AS (
        SELECT
            topic,
            -- Sum of negative counts in the last hour
            SUM(CASE 
                WHEN time > NOW() - INTERVAL '1 hour' THEN sentiment_count 
                ELSE 0 
            END) as recent_negative_count,
            
            -- Average negative counts per hour, over the last 24 hours
            (SUM(CASE 
                WHEN time > NOW() - INTERVAL '24 hour' THEN sentiment_count 
                ELSE 0 
            END) / 24.0) as baseline_avg_negative_count
            
        FROM topic_sentiment_rollups
        WHERE 
            topic = ANY(%s) -- Query all Tier 1 topics
            AND sentiment_label IN %s -- Only get negative labels
        GROUP BY topic
    )
    SELECT * FROM stats
    WHERE 
        recent_negative_count > 5 -- Only trigger if there's some volume
        AND baseline_avg_negative_count > 0 -- Avoid division by zero
    ;
    """
    
    conn = None
    try:
        conn = psycopg2.connect(TIMESCALE_DB_URL)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Convert TIER_1_TOPICS set to a list for the query
        cur.execute(query, (list(TIER_1_TOPICS), NEGATIVE_LABELS))
        results = cur.fetchall()
        
        if not results:
            print("No significant negative sentiment detected.")
            return "No alerts."

        alerts_triggered = 0
        for row in results:
            topic = row['topic']
            recent = row['recent_negative_count']
            baseline = row['baseline_avg_negative_count']
            
            # Check for division by zero (though our query should prevent it)
            if baseline == 0:
                continue
                
            change_factor = recent / baseline
            
            if change_factor > ALERT_THRESHOLD:
                percent_change = (change_factor - 1) * 100
                print("="*30)
                print(f"🚨🚨 CRISIS ALERT 🚨🚨")
                print(f"Topic:          {topic}")
                print(f"Sentiment:      Negative (Anger, Fear, etc.)")
                print(f"Spike:          +{percent_change:.0f}%")
                print(f"Details:        {recent:.0f} posts in last hour vs. 24h avg of {baseline:.1f} posts/hr")
                print("="*30)
                alerts_triggered += 1
        
        if alerts_triggered == 0:
            print("No alerts triggered for monitored topics.")
            
    except Exception as e:
        print(f"Error during crisis analysis: {e}")
    finally:
        if conn:
            conn.close()

    return f"Crisis analysis complete. {alerts_triggered} alerts triggered."
