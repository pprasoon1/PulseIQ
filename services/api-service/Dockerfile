# Use an official Python runtime
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
# We copy the 'app' folder into '/app/app' to match your docker-compose volume
COPY ./app ./app

# Default command (will be overridden by docker-compose, but good practice)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]