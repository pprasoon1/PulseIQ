from .celery_app import app
from .scrapers.reddit import RedditScraper
from .scrapers.gnews import GNewsScraper
import pymongo
import redis
import os

# --- Database & Cache Setup ---
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

# Initialize connections (Celery tasks are single-threaded)
try:
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client['pulseiq_db']
    raw_posts_collection = db['raw_posts']
except Exception as e:
    print(f"Failed to connect to Mongo: {e}")
    
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
except Exception as e:
    print(f"Failed to connect to Redis: {e}")

# --- Tiers (as per PRD) ---
# Tier 1: Always active (e.g., major brands) [cite: 34]
TIER_1_TOPICS = {"Tesla", "Apple", "iPhone 16", "Elon Musk", "OpenAI", "ChatGPT", "NVIDIA", "Google", "Sundar Pichai", "Microsoft", "Satya Nadella", "Meta", "Mark Zuckerberg", "Amazon", "Jeff Bezos", "SpaceX", "Narendra Modi", "Donald Trump", "Joe Biden", "Taylor Swift", "Cristiano Ronaldo", "Lionel Messi", "Virat Kohli", "Selena Gomez", "Samsung", "Galaxy S24", "Instagram", "YouTube", "Netflix", "Disney+", "Louis Vuitton", "Gucci",  "Nike", "Adidas"}

# Tier 2: Hot topics, 24h retention [cite: 35]
TIER_2_REDIS_KEY = "hot_topics"
# Tier 3: User searches, 72h retention [cite: 36]
TIER_3_REDIS_KEY = "priority_topics"

# --- Scraper Initialization ---
# Scrapers are instantiated globally so they can be reused by the worker
reddit_scraper = RedditScraper()
gnews_scraper = GNewsScraper()

# ==================================
# AUTONOMOUS ENGINE TASKS (run by Beat)
# ==================================

@app.task(name='discover_and_manage_topics')
def discover_and_manage_topics():
    """
    Autonomous engine main loop. Runs every hour. 
    1. Discovers new topics.
    2. Adds them to the Tier 2 list.
    """
    print("--- Running discover_and_manage_topics ---")
    try:
        hot_topics = gnews_scraper.get_top_topics()
        if hot_topics:
            # Use Redis pipeline to add all topics and set 24h expiry [cite: 28]
            pipe = redis_client.pipeline()
            for topic in hot_topics:
                # Add topic to the set
                pipe.sadd(TIER_2_REDIS_KEY, topic)
                # Set a key for the topic itself with 24h expiry
                pipe.set(f"topic:{topic}", "tier2", ex=24 * 3600)
            pipe.execute()
            print(f"Added {len(hot_topics)} new topics to Tier 2")
    except Exception as e:
        print(f"Error in discover_and_manage_topics: {e}")

@app.task(name='collect_hot_topics')
def collect_hot_topics():
    """
    Autonomous engine fast loop. Runs every 10 mins.
    This collects data for all *currently active* topics in Redis.
    """
    print("--- Running collect_hot_topics ---")
    
    # Get all topics from all tiers
    tier2_topics = redis_client.smembers(TIER_2_REDIS_KEY)
    tier3_topics = redis_client.smembers(TIER_3_REDIS_KEY)
    
    # Combine all unique topics
    all_active_topics = set()
    all_active_topics.update(TIER_1_TOPICS)
    all_active_topics.update(tier2_topics)
    all_active_topics.update(tier3_topics)
    
    print(f"Total active topics to scrape: {len(all_active_topics)}")
    
    # Dispatch collection tasks for each topic
    for topic in all_active_topics:
        # We dispatch individual tasks so they run in parallel
        # across all available workers.
        collect_data_for_topic.delay(topic)

# ==================================
# ON-DEMAND TASK (run by API)
# ==================================

@app.task(name='start_user_topic_collection')
def start_user_topic_collection(topic: str):
    """
    Fulfills "On-Demand User Search". 
    1. Adds topic to Tier 3 for 72 hours. [cite: 32]
    2. Immediately triggers data collection.
    """
    print(f"User request for topic: {topic}")
    try:
        # Use Redis pipeline for efficiency
        pipe = redis_client.pipeline()
        pipe.sadd(TIER_3_REDIS_KEY, topic) # Add to priority set
        pipe.set(f"topic:{topic}", "tier3", ex=72 * 3600) # Set 72h expiry [cite: 32]
        pipe.execute()
        print(f"Added '{topic}' to Tier 3")
        
        # Trigger immediate collection
        collect_data_for_topic.delay(topic)
    except Exception as e:
        print(f"Error in start_user_topic_collection: {e}")

# ==================================
# THE CORE WORKER TASK
# ==================================

@app.task(name='collect_data_for_topic')
def collect_data_for_topic(topic: str):
    """
    The main workhorse. Scrapes all sources for a single topic.
    """
    print(f"Collecting data for: {topic}")
    all_posts = []

    # Scrape Reddit
    try:
        reddit_posts = reddit_scraper.scrape_topic(topic)
        if reddit_posts:
            all_posts.extend(reddit_posts)
            print(f"Collected {len(reddit_posts)} posts from Reddit for '{topic}'")
    except Exception as e:
        print(f"Error scraping Reddit: {e}")
        
    # Scrape GNews
    try:
        gnews_posts = gnews_scraper.scrape_topic(topic)
        if gnews_posts:
            all_posts.extend(gnews_posts)
            print(f"Collected {len(gnews_posts)} posts from GNews for '{topic}'")
    except Exception as e:
        print(f"Error scraping GNews: {e}")

    # TODO: Add more scrapers here (Twitter, Blogs, etc.) [cite: 24]

    # Save to MongoDB
    if all_posts:
        try:
            # We use 'source_id' as a unique key.
            # This 'upsert' operation prevents duplicate posts.
            operations = [
                pymongo.UpdateOne(
                    {'source_id': post['source_id']},
                    {'$set': post, '$setOnInsert': {'first_seen': datetime.datetime.now(datetime.timezone.utc)}},
                    upsert=True
                )
                for post in all_posts if post.get('source_id')
            ]
            
            if operations:
                result = raw_posts_collection.bulk_write(operations)
                print(f"Saved to DB: {result.upserted_count} new, {result.modified_count} updated posts for '{topic}'")
            else:
                print(f"No posts with source_id found for '{topic}'")
                
        except Exception as e:
            print(f"Error saving posts to MongoDB: {e}")
    else:
        print(f"No new posts found for topic: {topic}")

    return f"Successfully collected {len(all_posts)} posts for: {topic}"