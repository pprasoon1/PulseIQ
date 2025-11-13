import os
import tweepy
import datetime

class TwitterScraper:
    def __init__(self):
        self.bearer_token = os.environ.get('X_BEARER_TOKEN')
        if not self.bearer_token:
            print("Warning: X_BEARER_TOKEN not set. Twitter scraper will not work.",self.bearer_token)
            self.client = None
        else:
            try:
                self.client = tweepy.Client(self.bearer_token)
                print("Twitter (X) scraper initialized")
            except Exception as e:
                print(f"Error initializing Tweepy client: {e}")
                self.client = None

    def scrape_topic(self, topic: str, limit: int = 20):
        if not self.client:
            return []
            
        print(f"Twitter: Scraping for '{topic}'")
        try:
            query = f'"{topic}" -is:retweet lang:en' # Added lang:en for better NLP
            
            response = self.client.search_recent_tweets(
                query=query,
                tweet_fields=["created_at", "public_metrics", "author_id", "lang"],
                user_fields=["location"],  # <-- NEW: Ask for user's location
                expansions=["author_id"], # <-- NEW: Tell API to include user objects
                max_results=limit
            )
            
            # --- NEW: Create a location lookup map ---
            user_locations = {}
            if response.includes and 'users' in response.includes:
                for user in response.includes['users']:
                    if user.location:
                        user_locations[user.id] = user.location
            # ----------------------------------------

            if not response.data:
                return []
                
            normalized_tweets = []
            for tweet in response.data:
                # Pass the location map to the normalizer
                user_loc = user_locations.get(tweet.author_id)
                normalized_tweets.append(self.normalize_tweet(tweet, topic, user_loc))
            return normalized_tweets
            
        except Exception as e:
            print(f"Error scraping Twitter for topic '{topic}': {e}")
            return []

    def normalize_tweet(self, tweet: tweepy.Tweet, topic: str, user_location: str = None):
        """Converts Tweepy Tweet object to our standard DB format."""
        
        # --- NEW: Add user_location to metadata ---
        metadata = {
            "author_id": tweet.author_id,
            "lang": tweet.lang,
            "retweet_count": tweet.public_metrics.get('retweet_count'),
            "reply_count": tweet.public_metrics.get('reply_count'),
            "like_count": tweet.public_metrics.get('like_count'),
            "impression_count": tweet.public_metrics.get('impression_count'),
        }
        if user_location:
            metadata['user_location_string'] = user_location
        # -----------------------------------------

        return {
            "topic": topic,
            "source": "twitter",
            "source_id": tweet.id,
            "text": tweet.text,
            "url": f"https://x.com/any/status/{tweet.id}",
            "timestamp": tweet.created_at,
            "metadata": metadata # Updated metadata
        }