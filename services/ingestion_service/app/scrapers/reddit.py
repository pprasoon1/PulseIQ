import os
import praw
import datetime

class RedditScraper:
    def __init__(self):
        client_id = os.environ.get('REDDIT_CLIENT_ID')
        client_secret = os.environ.get('REDDIT_CLIENT_SECRET')
        user_agent = os.environ.get('REDDIT_USER_AGENT')
        
        if not all([client_id, client_secret, user_agent]):
            print("Warning: Reddit credentials not set. Reddit scraper will not work.")
            self.reddit = None
        else:
            try:
                self.reddit = praw.Reddit(
                    client_id=client_id,
                    client_secret=client_secret,
                    user_agent=user_agent,
                )
                print("Reddit scraper initialized")
            except Exception as e:
                print(f"Error initializing PRAW: {e}")
                self.reddit = None

    def scrape_topic(self, topic: str, limit: int = 100):
        """Used by the worker to get posts for a specific topic."""
        if not self.reddit:
            return []
            
        print(f"Reddit: Scraping for '{topic}'")
        try:
            # Using subreddit("all") to search globally
            submissions = self.reddit.subreddit("all").search(topic, limit=limit)
            normalized_posts = []
            for sub in submissions:
                normalized_posts.append(self.normalize_submission(sub, topic))
            return normalized_posts
        except Exception as e:
            print(f"Error scraping Reddit for topic '{topic}': {e}")
            return []

    def normalize_submission(self, sub: praw.models.Submission, topic: str):
        """Converts PRAW submission format to our standard DB format."""
        return {
            "topic": topic,
            "source": "reddit",
            "source_id": sub.id,
            "text": f"{sub.title} {sub.selftext}",
            "url": f"https://www.reddit.com{sub.permalink}",
            "timestamp": datetime.datetime.fromtimestamp(sub.created_utc, tz=datetime.timezone.utc),
            "metadata": {
                "subreddit": str(sub.subreddit),
                "score": sub.score,
                "author": str(sub.author),
                "num_comments": sub.num_comments,
            }
        }