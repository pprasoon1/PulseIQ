from fastapi import FastAPI
from pydantic import BaseModel
from transformers import pipeline
from typing import List, Optional
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- 1. Load Models At Startup ---
print("Loading NLP models into memory...")
try:
    sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model="cardiffnlp/twitter-roberta-base-sentiment"
    )
    emotion_pipeline = pipeline(
        "text-classification",
        model="bhadresh-savani/bert-base-uncased-emotion"
    )
    print("NLP models loaded successfully.")

    # --- Initialize Geolocator ---
    print("Initializing geolocator...")
    geolocator = Nominatim(user_agent="pulseiq-nlp-service")
    # Rate-limited geocoding (to comply with Nominatim free tier)
    geocode_batch = RateLimiter(
        geolocator.geocode, 
        min_delay_seconds=1.1, 
        return_value_on_exception=None
    )
    print("Geolocator initialized successfully.")

except Exception as e:
    print(f"Error loading models or initializing geolocator: {e}")
    sentiment_pipeline = None
    emotion_pipeline = None
    geolocator = None

# --- 2. Label Mapping Dictionaries ---
SENTIMENT_MAP = {
    "LABEL_0": "NEGATIVE",
    "LABEL_1": "NEUTRAL",
    "LABEL_2": "POSITIVE"
}

EMOTION_MAP = {
    "anger": "ANGER",
    "disgust": "DISGUST",
    "fear": "FEAR",
    "joy": "JOY",
    "neutral": "NEUTRAL",
    "sadness": "SADNESS",
    "surprise": "SURPRISE"
}

# --- 3. Define Request & Response Data Models ---
class PostIn(BaseModel):
    """Incoming post for NLP analysis."""
    post_id: str
    text: str

class SentimentOut(BaseModel):
    label: str
    score: float

class EmotionOut(BaseModel):
    label: str
    score: float

class PostOut(BaseModel):
    """Outgoing post result with sentiment and emotion."""
    post_id: str
    sentiment: SentimentOut
    emotion: EmotionOut

# --- NEW: Geocoding Models ---
class GeoIn(BaseModel):
    post_id: str
    location_string: str

class GeoOut(BaseModel):
    post_id: str
    country_code: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None

# --- 4. Create FastAPI App ---
app = FastAPI(title="PulseIQ NLP Service")

@app.get("/", summary="Health check endpoint")
def read_root():
    """Check that NLP and geocoding models are loaded."""
    if sentiment_pipeline and emotion_pipeline and geolocator:
        return {"status": "ok", "message": "NLP Service is running and models/geolocator are loaded."}
    elif sentiment_pipeline and emotion_pipeline:
        return {"status": "partial", "message": "NLP models loaded but geolocator unavailable."}
    else:
        return {"status": "error", "message": "Models failed to load."}

# --- 5. NLP Batch Processing Endpoint ---
@app.post("/process", 
          response_model=List[PostOut], 
          summary="Process a batch of posts for sentiment and emotion")
async def process_posts(posts: List[PostIn]):
    """Process a list of posts and return sentiment & emotion results."""
    if not sentiment_pipeline or not emotion_pipeline:
        print("NLP pipelines not loaded.")
        return []

    results = []
    texts = [post.text for post in posts]

    try:
        sentiments = sentiment_pipeline(texts, truncation=True, max_length=512)
        emotions = emotion_pipeline(texts, truncation=True, max_length=512)

        for post, sentiment, emotion in zip(posts, sentiments, emotions):
            clean_sentiment = SENTIMENT_MAP.get(sentiment['label'], "UNKNOWN")
            clean_emotion = EMOTION_MAP.get(emotion['label'], "UNKNOWN")

            results.append(
                PostOut(
                    post_id=post.post_id,
                    sentiment=SentimentOut(label=clean_sentiment, score=sentiment['score']),
                    emotion=EmotionOut(label=clean_emotion, score=emotion['score'])
                )
            )
        return results

    except Exception as e:
        print(f"Error during NLP processing: {e}")
        return []

# --- 6. Geocoding Endpoint ---
@app.post("/process_geo",
          response_model=List[GeoOut],
          summary="Convert location strings into structured geo-data")
async def process_geo(locations: List[GeoIn]):
    """Convert raw location strings into country/state/city using Nominatim."""
    if not geolocator:
        print("Geolocator not initialized.")
        return []

    results = []
    for loc_in in locations:
        geo_out = GeoOut(post_id=loc_in.post_id)
        try:
            # Use the rate-limited geocoder
            location = geocode_batch(loc_in.location_string, addressdetails=True, language='en')

            if location and 'address' in location.raw:
                address = location.raw['address']
                geo_out.country_code = address.get('country_code', '').upper()
                geo_out.city = address.get('city') or address.get('town') or address.get('village')
                geo_out.state = address.get('state')

        except Exception as e:
            print(f"Error geocoding string '{loc_in.location_string}': {e}")

        results.append(geo_out)

    return results
