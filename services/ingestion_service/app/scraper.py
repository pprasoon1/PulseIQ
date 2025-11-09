import datetime

def scrape_topic(topic: str):
    """
    A placeholder function to simulate scraping data.
    In reality, this would use PRAW, a news API, or other scrapers.
    """
    print(f"--- Scraping for topic: {topic} ---")
    
    # Simulate finding 3 posts
    simulated_posts = [
        {
            "topic": topic,
            "source": "simulated-reddit",
            "text": f"I love {topic}, it's the best! #awesome",
            "timestamp": datetime.datetime.utcnow(),
            "location": "New York, NY"
        },
        {
            "topic": topic,
            "source": "simulated-news",
            "text": f"Breaking: {topic} announces new product. Market is reacting.",
            "timestamp": datetime.datetime.utcnow(),
            "location": "London, UK"
        },
        {
            "topic": topic,
            "source": "simulated-blog",
            "text": f"Why I'm selling my {topic} stock. It's not what it used to be.",
            "timestamp": datetime.datetime.utcnow(),
            "location": "San Francisco, CA"
        }
    ]
    
    print(f"--- Found {len(simulated_posts)} simulated posts ---")
    return simulated_posts