# syntax=docker/dockerfile:1
FROM python:3.9

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    gdal-bin \
    libgdal-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL environment variables
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Set MinIO environment variables with default values (can be overridden at runtime)
ENV MINIO_ENDPOINT_URL=http://minio:9000
ENV MINIO_ACCESS_KEY=minioadmin
ENV MINIO_SECRET_KEY=minioadmin
ENV MINIO_BUCKET_NAME=dagster-ndvi
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Create workspace.yaml for Kubernetes deployment and DAGSTER_HOME directory
RUN mkdir -p /opt/dagster/app /opt/dagster/dagster_home
COPY workspace.yaml /opt/dagster/app/workspace.yaml

# Create basic dagster.yaml config
RUN echo 'scheduler:' > /opt/dagster/dagster_home/dagster.yaml && \
    echo '  module: dagster.core.scheduler' >> /opt/dagster/dagster_home/dagster.yaml && \
    echo '  class: DagsterDaemonScheduler' >> /opt/dagster/dagster_home/dagster.yaml && \
    echo 'run_coordinator:' >> /opt/dagster/dagster_home/dagster.yaml && \
    echo '  module: dagster.core.run_coordinator' >> /opt/dagster/dagster_home/dagster.yaml && \
    echo '  class: QueuedRunCoordinator' >> /opt/dagster/dagster_home/dagster.yaml

# Copy requirements.txt first to leverage Docker caching
COPY requirements.txt .

# Install Python dependencies - CRITICAL: install the exact versions in requirements.txt first
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application
COPY . .

# Install the dagster_ndvi_project package
WORKDIR /app/dagster_ndvi_project
RUN pip install --no-cache-dir -e .

# Go back to the app directory
WORKDIR /app

# Install the package
RUN pip install --no-cache-dir --no-deps .

# Create entrypoint script
RUN echo '#!/bin/bash' > /entrypoint.sh && \
    echo 'if [ "$1" = "grpc" ]; then' >> /entrypoint.sh && \
    echo '  exec dagster api grpc -h 0.0.0.0 -p 4000 -m dagster_ndvi_project' >> /entrypoint.sh && \
    echo 'elif [ "$1" = "daemon" ]; then' >> /entrypoint.sh && \
    echo '  exec dagster-daemon run -m dagster_ndvi_project' >> /entrypoint.sh && \
    echo 'else' >> /entrypoint.sh && \
    echo '  exec dagster dev -h 0.0.0.0 -p 3000 -m dagster_ndvi_project' >> /entrypoint.sh && \
    echo 'fi' >> /entrypoint.sh && \
    chmod +x /entrypoint.sh

# Default command 
ENTRYPOINT ["/entrypoint.sh"]
CMD ["dev"] 