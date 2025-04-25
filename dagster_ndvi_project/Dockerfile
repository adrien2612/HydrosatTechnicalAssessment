# Dockerfile for Dagster NDVI pipeline

FROM python:3.10-slim

# Install system libraries for geospatial processing
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      gdal-bin \
      libgdal-dev \
      libproj-dev && \
    rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app


# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Dagster project code into the image
COPY dagster_ndvi_project/ ./dagster_ndvi_project/
COPY definitions.py ./definitions.py

# Ensure PYTHONPATH includes the app root
ENV PYTHONPATH=/app

# Default entrypoint (overridden by k3d deployment commands)
ENTRYPOINT ["bash", "-l"]
