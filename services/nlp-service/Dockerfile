# Use an official Python runtime
FROM python:3.10-slim

WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- This is the new, efficient step ---
# Copy and run the preload script to download models into the image layer
COPY preload_models.py .
RUN python preload_models.py

# Copy the main application code
COPY main.py .

# Command to run the service (will be overridden by docker-compose)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]