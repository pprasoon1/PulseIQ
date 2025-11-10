import os
import tweepy
import datetime

class TwitterScraper:
    def __init__(self):
        self.bearer_token = os.environ.get('X_BEARER_TOKEN')
        if not self.bearer_token:
            print("Warning: X_BEARER_TOKEN not set. Twitter scraper will not work.")
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
            # -is:retweet prevents retweets from cluttering results
            query = f'"{topic}" -is:retweet'
            
            response = self.client.search_recent_tweets(
                query=query,
                tweet_fields=["created_at", "public_metrics", "author_id", "lang"],
                max_results=limit
            )
            
            if not response.data:
                return []
                
            normalized_tweets = []
            for tweet in response.data:
                normalized_tweets.append(self.normalize_tweet(tweet, topic))
            return normalized_tweets
            
        except Exception as e:
            print(f"Error scraping Twitter for topic '{topic}': {e}")
            return []

    def normalize_tweet(self, tweet: tweepy.Tweet, topic: str):
        """Converts Tweepy Tweet object to our standard DB format."""
        return {
            "topic": topic,
            "source": "twitter",
            "source_id": tweet.id,
            "text": tweet.text,
            "url": f"https://x.com/any/status/{tweet.id}",
            "timestamp": tweet.created_at,
            "metadata": {
                "author_id": tweet.author_id,
                "lang": tweet.lang,
                "retweet_count": tweet.public_metrics.get('retweet_count'),
                "reply_count": tweet.public_metrics.get('reply_count'),
                "like_count": tweet.public_metrics.get('like_count'),
                "impression_count": tweet.public_metrics.get('impression_count'),
            }
        }