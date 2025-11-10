from transformers import pipeline

print("Downloading sentiment model...")
pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment")
print("Sentiment model downloaded.")

print("Downloading emotion model...")
pipeline("text-classification", model="bhadresh-savani/bert-base-uncased-emotion")
print("Emotion model downloaded.")

print("All models downloaded and cached.")