import requests
import datetime

class YCScraper:
    BASE_URL = "http://hn.algolia.com/api/v1/search"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'PulseIQ v0.1'})
        print("Hacker News (YC) scraper initialized")

    def scrape_topic(self, topic: str, limit: int = 20):
        print(f"Hacker News: Scraping for '{topic}'")
        params = {
            'query': topic,
            'tags': '(story,comment)',
            'hitsPerPage': limit
        }
        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status() # Raise error for bad responses (4xx, 5xx)
            
            hits = response.json().get('hits', [])
            normalized_posts = []
            for hit in hits:
                normalized_posts.append(self.normalize_hit(hit, topic))
            return normalized_posts
            
        except requests.RequestException as e:
            print(f"Error scraping Hacker News for topic '{topic}': {e}")
            return []

    def normalize_hit(self, hit: dict, topic: str):
        """Converts Algolia HN hit to our standard DB format."""
        
        # Get text, preferring comment_text, then story_title, then title
        text = hit.get('comment_text', hit.get('story_title', hit.get('title', '')))
        
        # Get URL, preferring story_url, then providing the HN link
        url = hit.get('story_url')
        if not url:
            url = f"https://news.ycombinator.com/item?id={hit.get('objectID')}"

        return {
            "topic": topic,
            "source": "hackernews",
            "source_id": hit.get('objectID'),
            "text": text,
            "url": url,
            "timestamp": datetime.datetime.fromtimestamp(hit.get('created_at_i'), tz=datetime.timezone.utc),
            "metadata": {
                "author": hit.get('author'),
                "points": hit.get('points'),
                "num_comments": hit.get('num_comments'),
                "tags": hit.get('_tags', []),
            }
        }