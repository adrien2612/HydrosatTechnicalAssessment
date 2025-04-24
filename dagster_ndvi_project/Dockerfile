FROM python:3.10-slim AS builder

# Set environment variables for build
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=0 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies for geo libraries (build stage only)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgdal-dev \
    gdal-bin \
    libproj-dev \
    libgeos-dev \
    libspatialindex-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set up working directory
WORKDIR /build

# Copy only pyproject.toml first for better layer caching
COPY pyproject.toml .

# Create virtual environment and install dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install dependencies in specific order with optimizations
# Explicitly use binary wheels where possible and only build from source when needed
RUN pip install --upgrade pip wheel setuptools && \
    # Install boto dependencies first
    pip install --no-cache-dir botocore==1.29.161 boto3==1.26.161 aiobotocore==2.5.2 && \
    # Install dagster dependencies
    pip install --no-cache-dir dagster==1.10.11 dagster-aws==0.26.11 dagster-k8s==0.26.11 dagster-postgres==0.26.11 dagster-webserver==1.10.11 && \
    # Install geospatial libraries with binary preference
    pip install --prefer-binary --no-cache-dir \
        pystac-client==0.7.5 \
        planetary-computer==1.0.0 \
        geopandas==0.13.2 \
        shapely==2.0.2 \
        pyproj==3.6.1 && \
    # Install GDAL-dependent packages with binary preference
    GDAL_CONFIG=/usr/bin/gdal-config pip install --prefer-binary --no-cache-dir \
        rasterio==1.3.8 && \
    # Install remaining dependencies
    pip install --no-cache-dir \
        numpy==1.24.4 \
        pandas==2.0.3 \
        scipy==1.10.1 \
        matplotlib==3.7.3 \
        tqdm==4.66.1 \
        python-dateutil==2.8.2

# Final stage
FROM python:3.10-slim

# Runtime dependencies only - using correct package names available in Debian bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal32 \
    libgeos-c1v5 \
    libproj-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

# Set up working directory
WORKDIR /opt/dagster/app

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Copy project files
COPY . .

# Install our package in development mode
RUN pip install -e .

# Create workspace.yaml
RUN mkdir -p /opt/dagster/app/workspace
COPY workspace.yaml /opt/dagster/app/workspace/workspace.yaml

# Create entrypoint script
RUN echo '#!/bin/bash' > /entrypoint.sh && \
    echo 'if [ "$1" = "grpc" ]; then' >> /entrypoint.sh && \
    echo '  exec dagster api grpc -h 0.0.0.0 -p 4000 -m dagster_ndvi_project' >> /entrypoint.sh && \
    echo 'else' >> /entrypoint.sh && \
    echo '  exec dagster dev -h 0.0.0.0 -p 3000 -m dagster_ndvi_project' >> /entrypoint.sh && \
    echo 'fi' >> /entrypoint.sh && \
    chmod +x /entrypoint.sh

# Default command to run Dagster code server
ENTRYPOINT ["/entrypoint.sh"]
CMD ["grpc"] 