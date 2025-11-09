# Use an official Python runtime
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the application code into the container
COPY ./app .

# Default command (will be overridden by docker-compose, but good practice)
CMD ["celery", "-A", "app.tasks", "worker", "--loglevel=info"]