import os
from gnews import GNews
import datetime
from datetime import timezone

# This scraper will be used for both discovery (top news)
# and for collecting news on a specific topic.

# REPLACE WITH THIS:
class GNewsScraper:
    def __init__(self):
        # We read the API key from the environment
        self.api_key = os.environ.get('GNEWS_API_KEY')
        if not self.api_key:
            print("Warning: GNEWS_API_KEY not set. GNews scraper will not work.")
            self.google_news = None
        else:
            self.google_news = GNews() # <-- 1. Create it empty
            self.google_news.api_key = self.api_key # <-- 2. Set the key here
            self.google_news.language = 'en'
            self.google_news.country = 'US'
            self.google_news.max_results = 20

    def get_top_topics(self):
        """
        Used by the autonomous engine to find new 'hot' topics.
        NEW: This now discovers *source names* (e.g., 'Reuters', 'Politico')
        and adds them as topics.
        """
        if not self.google_news:
            return []

        try:
            news = self.google_news.get_top_news()

            # Use a set to avoid duplicate source names
            topics = set()
            for article in news:
                source_name = article.get('source', {}).get('name')
                if source_name:
                    topics.add(source_name)

            print(f"Discovered {len(topics)} hot topics (sources) from GNews: {topics}")
            return list(topics) # Return as a list

        except Exception as e:
            print(f"Error fetching top GNews topics: {e}")
            return []

    def scrape_topic(self, topic: str):

        """Used by the worker to get news for a specific topic."""
        if not self.google_news:
            return []
            
        print(f"GNews: Scraping for '{topic}'")
        try:
            articles = self.google_news.get_news(topic)
            normalized_posts = []
            for article in articles:
                normalized_posts.append(self.normalize_article(article, topic))
            return normalized_posts
        except Exception as e:
            print(f"Error scraping GNews for topic '{topic}': {e}")
            return []

    def normalize_article(self, article: dict, topic: str):
        """Converts GNews article format to our standard DB format."""

        # Try to parse the date string
        try:
            timestamp = datetime.strptime(article.get('published at'), '%Y-%m-%dT%H:%M:%SZ')
        except (ValueError, TypeError):
            # If parsing fails or date is missing, use current time as a fallback
            timestamp = datetime.datetime.now(datetime.timezone.utc)


        return {
            "topic": topic,
            "source": "gnews",
            "source_id": article.get('url'),
            "text": f"{article.get('title', '')}. {article.get('description', '')}",
            "url": article.get('url'),
            "timestamp": timestamp, # Use the parsed timestamp
            "metadata": {
                "source_name": article.get('source', {}).get('name'),
                "publisher": article.get('publisher', {}).get('title'),
                "image": article.get('image'),
            }
        }