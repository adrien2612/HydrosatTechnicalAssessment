# Dagster NDVI Processing Pipeline

This project implements a Normalized Difference Vegetation Index (NDVI) processing pipeline using Dagster, deployed on Kubernetes. The pipeline processes satellite imagery data to calculate vegetation indices for agricultural fields.

## Project Overview

The NDVI pipeline performs the following operations:
- Monitors a MinIO bucket for new field geometry files
- Registers field partitions dynamically
- Fetches Sentinel-2 satellite data via STAC API
- Computes NDVI values for each field
- Generates time series reports and anomaly detection

## What is NDVI?

NDVI (Normalized Difference Vegetation Index) is a simple graphical indicator used to analyze remote sensing measurements, typically from satellite imagery, to assess whether the target being observed contains live green vegetation. The calculation uses the visible and near-infrared light reflected by vegetation:

```
NDVI = (NIR - RED) / (NIR + RED)
```

- Healthy vegetation absorbs most visible light and reflects a large portion of near-infrared light, resulting in high NDVI values (0.6 to 0.9)
- Unhealthy or sparse vegetation reflects more visible light and less near-infrared light, giving moderate NDVI values (0.2 to 0.5)
- Bare soils and non-vegetated areas have NDVI values close to 0
- Water bodies have negative NDVI values

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
├── k8s/deployment/                 # Kubernetes deployment templates
├── input_data/                     # Sample input data for testing
├── Dockerfile                      # Docker image definition
├── requirements.txt                # Python dependencies
├── workspace.yaml                  # Dagster workspace configuration
├── setup.sh                        # Script to setup MinIO and dependencies
└── deploy_dagster.sh               # Deployment script for Kubernetes
```

## Prerequisites

- Kubernetes cluster (can be local using k3d, minikube, etc.)
- Docker
- kubectl command-line tool
- MinIO instance (or S3-compatible storage)

## Complete Deployment Workflow

### 1. Initial Setup with setup.sh

The `setup.sh` script automates the initial setup process, including:
- Creating a k3d cluster if it doesn't exist
- Deploying MinIO to Kubernetes
- Setting up the required bucket and folders
- Configuring the necessary Kubernetes resources

```bash
# Make the script executable
chmod +x setup.sh

# Run the setup script
./setup.sh
```

### 2. Build and Deploy Dagster

```bash
# Make the deployment script executable
chmod +x deploy_dagster.sh

# Run the deployment script
./deploy_dagster.sh
```

### 3. Access the Dagit UI

Once the deployment is complete, access the Dagit UI to monitor and manage your pipeline:

```bash
kubectl -n dagster port-forward svc/dagit 3000:80
```

Then visit http://localhost:3000 in your browser to access the Dagit UI.

## Using the Pipeline

### 1. Accessing MinIO

MinIO provides a web interface to manage your buckets and files:

```bash
kubectl -n dagster port-forward svc/minio 9000:9000
```

Then visit http://localhost:9000 in your browser, and log in with:
- Username: minioadmin
- Password: minioadmin

### 2. Upload Input Files

Upload the following files to the `dagster-test-bucket/input_data/` folder in MinIO:

1. **fields.geojson**: Contains field geometries with required properties
   - Each field must have a `field_id` and `planting_date`
   
2. **bounding_box.geojson**: Contains an area of interest bounding box

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

### 3. Monitor Pipeline Execution

1. Open the Dagit UI at http://localhost:3000
2. Navigate to the "Sensors" tab to see the `optimized_ndvi_sensor`
3. The sensor will automatically detect new/modified input files and trigger the pipeline
4. You can also manually trigger the pipeline from the "Jobs" tab

### 4. View Output Files

The pipeline generates several outputs in the MinIO bucket:

- **Output Data**: `dagster-test-bucket/output_data/`
  - NDVI data for each field: `output_data/ndvi/{field_id}/{date}/ndvi.nc`
  - Daily summaries: `output_data/summary/{date}/daily_summary.json`
  - Time series reports: `output_data/timeseries/{field_id}/report.csv`

You can download these files from the MinIO web interface or access them programmatically.

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

## Future Development

### 1. Vault Integration

To improve security, future versions will integrate with HashiCorp Vault for secrets management:

- Store MinIO credentials in Vault instead of Kubernetes secrets
- Configure Dagster to fetch credentials from Vault at runtime
- Implement periodic credential rotation

Implementation plan:
```
1. Deploy Vault in Kubernetes cluster
2. Configure Vault for Kubernetes authentication
3. Create a Vault resource in Dagster
4. Update resources to fetch credentials from Vault
```

### 2. Enhanced Logging

Planned logging improvements:

- Structured logging with JSON format
- Integration with centralized logging systems (ELK, Loki)
- Detailed performance metrics for each processing step
- Audit trail for data access and modifications

### 3. Performance Optimizations

- Implement parallel processing for multiple fields
- Add caching layer for frequently accessed satellite data
- Optimize NDVI calculation for large datasets using dask
- Configure resource autoscaling based on workload

## License

This project is licensed under the MIT License - see the LICENSE file for details. 