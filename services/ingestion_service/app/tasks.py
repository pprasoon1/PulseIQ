from datetime import datetime, timezone
from .celery_app import app
from .scrapers.reddit import RedditScraper
from .scrapers.gnews import GNewsScraper
from .scrapers.twitter import TwitterScraper
from .scrapers.yc import YCScraper
import pymongo
import redis
import os
import requests # <-- NEW IMPORT for calling nlp-service

# --- Database & Cache Setup ---
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

# --- NLP Service URL ---
NLP_SERVICE_URL = "http://nlp-service:8001/process" # Docker service name

# Initialize connections
try:
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client['pulseiq_db']
    raw_posts_collection = db['raw_posts']
    analyzed_posts_collection = db['analyzed_posts'] # <-- NEW COLLECTION
except Exception as e:
    print(f"Failed to connect to Mongo: {e}")
    
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
except Exception as e:
    print(f"Failed to connect to Redis: {e}")

# ... (TIER definitions and Scraper initializations are unchanged) ...
TIER_1_TOPICS = {"Tesla", "Apple", "iPhone 16", "Elon Musk", "OpenAI", "ChatGPT", "NVIDIA"}
TIER_2_REDIS_KEY = "hot_topics"
TIER_3_REDIS_KEY = "priority_topics"

reddit_scraper = RedditScraper()
gnews_scraper = GNewsScraper()
twitter_scraper = TwitterScraper()
yc_scraper = YCScraper()

# ... ('discover_and_manage_topics', 'collect_hot_topics', 'start_user_topic_collection' are unchanged) ...
# (We'll skip them here for brevity, but leave them in your file)

@app.task(name='discover_and_manage_topics')
def discover_and_manage_topics():
    # ... (no changes) ...
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
    # ... (no changes) ...
    print("--- Running collect_hot_topics ---")
    tier2_topics = redis_client.smembers(TIER_2_REDIS_KEY)
    tier3_topics = redis_client.smembers(TIER_3_REDIS_KEY)
    all_active_topics = set()
    all_active_topics.update(TIER_1_TOPICS)
    all_active_topics.update(tier2_topics)
    all_active_topics.update(tier3_topics)
    print(f"Total active topics to scrape: {len(all_active_topics)}")
    for topic in all_active_topics:
        collect_data_for_topic.delay(topic)

@app.task(name='start_user_topic_collection')
def start_user_topic_collection(topic: str):
    # ... (no changes) ...
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


# ==================================
# THE CORE WORKER TASK (collect_data_for_topic)
# ==================================
@app.task(name='collect_data_for_topic')
def collect_data_for_topic(topic: str):
    """
    Workhorse 1: Scrapes all sources and saves to 'raw_posts'.
    """
    print(f"Collecting data for: {topic}")
    all_posts = []

    # ... (Scraping logic is unchanged) ...
    try:
        reddit_posts = reddit_scraper.scrape_topic(topic)
        if reddit_posts:
            all_posts.extend(reddit_posts)
            print(f"Collected {len(reddit_posts)} posts from Reddit for '{topic}'")
    except Exception as e:
        print(f"Error scraping Reddit: {e}")
    try:
        gnews_posts = gnews_scraper.scrape_topic(topic)
        if gnews_posts:
            all_posts.extend(gnews_posts)
            print(f"Collected {len(gnews_posts)} posts from GNews for '{topic}'")
    except Exception as e:
        print(f"Error scraping GNews: {e}")
    try:
        twitter_posts = twitter_scraper.scrape_topic(topic)
        if twitter_posts:
            all_posts.extend(twitter_posts)
            print(f"Collected {len(twitter_posts)} posts from Twitter for '{topic}'")
    except Exception as e:
        print(f"Error scraping Twitter: {e}")
    try:
        yc_posts = yc_scraper.scrape_topic(topic)
        if yc_posts:
            all_posts.extend(yc_posts)
            print(f"Collected {len(yc_posts)} posts from Hacker News for '{topic}'")
    except Exception as e:
        print(f"Error scraping Hacker News: {e}")
        
    # --- MODIFIED SAVE LOGIC ---
    if all_posts:
        try:
            operations = [
                pymongo.UpdateOne(
                    {'source_id': post['source_id']},
                    {
                        '$set': post,
                        '$setOnInsert': {
                            'first_seen': datetime.now(timezone.utc),
                            'nlp_processed': False # <-- NEW FLAG
                        },
                    },
                    upsert=True
                )
                for post in all_posts if post.get('source_id')
            ]

            if operations:
                result = raw_posts_collection.bulk_write(operations)
                print(f"Saved to 'raw_posts': {result.upserted_count} new, {result.modified_count} updated posts for '{topic}'")
            else:
                print(f"No posts with source_id found for '{topic}'")

        except Exception as e:
            print(f"Error saving posts to MongoDB: {e}")
    else:
        print(f"No new posts found for topic: {topic}")

    return f"Successfully collected {len(all_posts)} posts for: {topic}"


# ==================================
# NEW NLP WORKER TASK (PHASE 2)
# ==================================
@app.task(name='process_raw_posts')
def process_raw_posts(batch_size: int = 100):
    """
    Workhorse 2: Processes raw posts and saves to 'analyzed_posts'.
    """
    print("--- Running process_raw_posts ---")
    
    # 1. Find raw posts that have not been processed
    posts_to_process = list(raw_posts_collection.find(
        {"nlp_processed": False}
    ).limit(batch_size))
    
    if not posts_to_process:
        print("No new posts to process.")
        return "No new posts."
        
    print(f"Found {len(posts_to_process)} posts to process.")

    # 2. Prepare payload for the NLP service
    payload = []
    for post in posts_to_process:
        payload.append({
            "post_id": str(post['_id']),
            "text": post.get('text', '')
        })

    # 3. Call the NLP service
    try:
        response = requests.post(NLP_SERVICE_URL, json=payload, timeout=30)
        response.raise_for_status() # Raise error for 4xx/5xx
        nlp_results = response.json()
    except requests.RequestException as e:
        print(f"Error calling NLP service: {e}")
        return f"NLP service call failed."

    # 4. Prepare batch operations for MongoDB
    analyzed_ops = []
    raw_update_ids = []
    
    # Create a lookup map for results
    results_map = {res['post_id']: res for res in nlp_results}

    for post in posts_to_process:
        post_id_str = str(post['_id'])
        result = results_map.get(post_id_str)
        
        if result:
            # Copy all original fields
            analyzed_post = post.copy()
            
            # Add the new NLP fields
            analyzed_post['sentiment'] = result['sentiment']
            analyzed_post['emotion'] = result['emotion']
            
            # Remove the 'nlp_processed' flag for the final collection
            analyzed_post.pop('nlp_processed', None)
            
            # Prepare to insert into 'analyzed_posts'
            # We use 'upsert' to be safe, just in case a task runs twice
            analyzed_ops.append(
                pymongo.UpdateOne(
                    {'_id': post['_id']},
                    {'$set': analyzed_post},
                    upsert=True
                )
            )
            
            # Mark this ID for update in 'raw_posts'
            raw_update_ids.append(post['_id'])

    # 5. Execute batch operations
    try:
        if analyzed_ops:
            # Save to 'analyzed_posts'
            analyzed_result = analyzed_posts_collection.bulk_write(analyzed_ops)
            print(f"Saved to 'analyzed_posts': {analyzed_result.upserted_count} new, {analyzed_result.modified_count} updated.")
            
            # Update 'raw_posts' to mark as processed
            raw_update_result = raw_posts_collection.update_many(
                {'_id': {'$in': raw_update_ids}},
                {'$set': {'nlp_processed': True}}
            )
            print(f"Updated 'raw_posts': {raw_update_result.modified_count} marked as processed.")
            
    except Exception as e:
        print(f"Error saving NLP results to MongoDB: {e}")

    return f"Successfully processed {len(nlp_results)} posts."