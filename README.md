# Dagster NDVI Processing Pipeline

This project implements a Normalized Difference Vegetation Index (NDVI) processing pipeline using Dagster, deployed on Kubernetes. The pipeline processes satellite imagery data to calculate vegetation indices for agricultural fields.

## Project Overview

The NDVI pipeline performs the following operations:
- Monitors a MinIO bucket for new field geometry files
- Registers field partitions dynamically
- Fetches Sentinel-2 satellite data via STAC API
- Computes NDVI values for each field
- Generates time series reports and anomaly detection

## Project Structure

```
.
├── dagster_ndvi_project/           # Main Dagster package
│   ├── dagster_ndvi_project/       # Core code package
│   │   ├── assets.py               # Asset definitions (NDVI processing)
│   │   ├── resources.py            # Resource definitions (MinIO connectivity)
│   │   ├── definitions.py          # Job and repository definitions
│   │   └── sensors/                # Sensor definitions
│   │       └── optimized_ndvi_sensor.py  # Sensor to detect new input files
│   ├── __init__.py                 # Package initialization
│   └── setup.py                    # Package setup
├── k8s/                            # Kubernetes deployment templates (OPTIONAL)
├── input_data/                     # Sample input data for testing
├── Dockerfile                      # Docker image definition
├── requirements.txt                # Python dependencies
├── workspace.yaml                  # Dagster workspace configuration
└── deplo_new_dagster.sh            # Deployment script for Kubernetes
```

## Prerequisites

- Kubernetes cluster (can be local using k3d, minikube, etc.)
- Docker
- kubectl command-line tool
- MinIO instance (or S3-compatible storage)

## Quick Start

### 1. Build the Docker Image

```bash
docker build -t dagster-ndvi:latest .
```

### 2. Import the Image to K3d (if using k3d)

```bash
k3d image import dagster-ndvi:latest -c <your-cluster-name>
```

### 3. Deploy to Kubernetes

```bash
./deplo_new_dagster.sh
```

### 4. Access the Dagit UI

```bash
kubectl -n dagster port-forward svc/dagit 3000:80
```

Then visit http://localhost:3000 in your browser.

## Required Environment Variables

The following environment variables are required for the pipeline to run:

- `MINIO_ENDPOINT_URL`: URL for the MinIO service
- `MINIO_ACCESS_KEY`: Access key for MinIO
- `MINIO_SECRET_KEY`: Secret key for MinIO
- `MINIO_BUCKET_NAME`: Bucket name for storing input and output data

These are configured in the Kubernetes deployment files.

## Input Data Format

The pipeline expects two GeoJSON files in the MinIO bucket at the following paths:
- `input_data/fields.geojson`: Contains field geometries with `field_id` and `planting_date` properties
- `input_data/bounding_box.geojson`: Contains an area of interest bounding box

Example fields.geojson structure:
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "field_id": "F001",
        "planting_date": "2023-03-15"
      },
      "geometry": {...}
    }
  ]
}
```

## Recent Improvements

- Added partition key safety checks to prevent AttributeError exceptions
- Fixed method name in timeseries report asset
- Improved sensor logic to properly trigger the ndvi_processing_job
- Ensured proper field_id resolution between string and numeric formats
- Added comprehensive logging for troubleshooting

## Troubleshooting

### Check Pod Status
```bash
kubectl -n dagster get pods
```

### View Pod Logs
```bash
kubectl -n dagster logs <pod-name>
```

### Check Sensor Logs
```bash
kubectl -n dagster logs -f <dagster-daemon-pod-name>
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 