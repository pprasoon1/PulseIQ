import os
from gnews import GNews
from datetime import datetime, timezone


class GNewsScraper:
    """
    This scraper uses the GNews API to:
      - Discover trending topics from top news sources
      - Scrape recent news articles for a specific topic
    """

    def __init__(self):
        # Read the API key from environment variables
        self.api_key = os.environ.get('GNEWS_API_KEY')

        if not self.api_key:
            print("Warning: GNEWS_API_KEY not set. GNews scraper will not work.")
            self.google_news = None
        else:
            # Initialize GNews client
            self.google_news = GNews()
            self.google_news.api_key = self.api_key
            self.google_news.language = 'en'
            self.google_news.country = 'US'
            self.google_news.max_results = 20

    def get_top_topics(self):
        """
        Discover top news sources (used by the autonomous engine).
        Returns a list of unique source names (e.g., 'Reuters', 'BBC News').
        """
        if not self.google_news:
            return []

        try:
            news = self.google_news.get_top_news()

            # Extract unique source names
            topics = set()
            for article in news:
                source_name = article.get('source', {}).get('name')
                if source_name:
                    topics.add(source_name)

            print(f"Discovered {len(topics)} hot topics (sources) from GNews: {topics}")
            return list(topics)

        except Exception as e:
            print(f"Error fetching top GNews topics: {e}")
            return []

    def scrape_topic(self, topic: str):
        """
        Fetch recent articles for a given topic.
        Returns a list of normalized article dictionaries ready for MongoDB.
        """
        if not self.google_news:
            return []

        print(f"GNews: Scraping for '{topic}'")
        try:
            articles = self.google_news.get_news(topic)
            normalized_posts = [
                self.normalize_article(article, topic)
                for article in articles
            ]
            return normalized_posts
        except Exception as e:
            print(f"Error scraping GNews for topic '{topic}': {e}")
            return []

    def normalize_article(self, article: dict, topic: str):
        """
        Convert a GNews article to the standard PulseIQ post format.
        Handles missing timestamps safely.
        """
        try:
            # Parse published date if available
            published_at = article.get('published at')
            if published_at:
                timestamp = datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ')
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            else:
                timestamp = datetime.now(timezone.utc)
        except (ValueError, TypeError):
            timestamp = datetime.now(timezone.utc)

        return {
            "topic": topic,
            "source": "gnews",
            "source_id": article.get('url'),
            "text": f"{article.get('title', '')}. {article.get('description', '')}",
            "url": article.get('url'),
            "timestamp": timestamp,
            "metadata": {
                "source_name": article.get('source', {}).get('name'),
                "publisher": article.get('publisher', {}).get('title'),
                "image": article.get('image'),
            },
        }
