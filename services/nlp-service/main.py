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
    
    # Get all texts for batch processing (more efficient)
    texts = [post.text for post in posts]

    try:
        # Run batch analysis
        sentiments = sentiment_pipeline(texts)
        emotions = emotion_pipeline(texts)
        
        # Combine results
        for post, sentiment, emotion in zip(posts, sentiments, emotions):
            results.append(
                PostOut(
                    post_id=post.post_id,
                    sentiment=SentimentOut(label=sentiment['label'], score=sentiment['score']),
                    emotion=EmotionOut(label=emotion['label'], score=emotion['score'])
                )
            )
            
        return results
        
    except Exception as e:
        print(f"Error during NLP processing: {e}")
        return []