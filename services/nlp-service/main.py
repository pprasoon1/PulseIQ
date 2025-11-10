from fastapi import FastAPI
from pydantic import BaseModel
from transformers import pipeline
from typing import List

# --- 1. Load Models At Startup ---
# Models were pre-downloaded by preload_models.py, so this is fast.
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
except Exception as e:
    print(f"Error loading models: {e}")
    sentiment_pipeline = None
    emotion_pipeline = None

# --- NEW: Label Mapping Dictionaries ---
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

# --- 2. Define Request & Response Data Models ---
class PostIn(BaseModel):
    """The data we expect to receive for one post."""
    post_id: str
    text: str

class SentimentOut(BaseModel):
    label: str
    score: float

class EmotionOut(BaseModel):
    label: str
    score: float

class PostOut(BaseModel):
    """The data we will send back for one post."""
    post_id: str
    sentiment: SentimentOut
    emotion: EmotionOut

# --- 3. Create FastAPI App ---
app = FastAPI(title="PulseIQ NLP Service")

@app.get("/", summary="Health check endpoint")
def read_root():
    if sentiment_pipeline and emotion_pipeline:
        return {"status": "ok", "message": "NLP Service is running and models are loaded."}
    else:
        return {"status": "error", "message": "NLP models failed to load."}

@app.post("/process", 
          response_model=List[PostOut], 
          summary="Process a batch of posts for sentiment and emotion")
async def process_posts(posts: List[PostIn]):
    """
    Receives a list of posts and returns analysis for each.
    """
    if not sentiment_pipeline or not emotion_pipeline:
        return []

    results = []
    texts = [post.text for post in posts]

    try:
        sentiments = sentiment_pipeline(texts, truncation=True, max_length=512)
        emotions = emotion_pipeline(texts, truncation=True, max_length=512)
        
        for post, sentiment, emotion in zip(posts, sentiments, emotions):
            
            # --- FIX: Use the map to get clean labels ---
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