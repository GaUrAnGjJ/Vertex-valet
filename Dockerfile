# Use an official Python runtime with a specific version
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Set environment variables to prevent Python from buffering output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Initialize database if it doesn't exist
RUN if [ ! -f storage/library.db ]; then echo "Building database..."; python pipeline.py --db; fi

# Build embeddings if they don't exist
RUN if [ ! -f recommender/embeddings.pkl ]; then echo "Building embeddings..."; python recommender/build_embeddings.py; fi

# Expose port 8000 for the API
EXPOSE 8000

# Command to run the application using the PORT environment variable
CMD sh -c "uvicorn API.main:app --host 0.0.0.0 --port ${PORT:-8000}"
