# Dagster NDVI Processing Pipeline

This project implements a Normalized Difference Vegetation Index (NDVI) processing pipeline using Dagster, deployed on Kubernetes. The pipeline processes satellite imagery data to calculate vegetation indices for agricultural fields.

## Project Overview

The NDVI pipeline performs the following operations:
- Monitors a MinIO bucket for new field geometry files
- Registers field partitions dynamically
- Fetches Sentinel-2 satellite data via STAC API
- Computes NDVI values for each field
- Generates time series reports and anomaly detection

## Pipeline Assets & Data Flow

The pipeline consists of several interconnected assets that process data in a specific sequence:

### 1. `load_bounding_box` Asset
- **Purpose**: Loads the study area boundary from a GeoJSON file
- **Input**: `input_data/bounding_box.geojson` from MinIO
- **Processing**: Parses the GeoJSON and converts it to a Shapely geometry
- **Output**: A dictionary containing the geometry and its bounds
- **Triggers**: Materializes when files are detected by the sensor

### 2. `load_fields` Asset
- **Purpose**: Loads field boundaries and creates field partitions dynamically
- **Input**: `input_data/fields.geojson` from MinIO
- **Processing**: 
  - Validates required columns (`field_id`, `planting_date`)
  - Registers each field as a dynamic partition
  - Parses dates and converts geometries
- **Output**: A GeoDataFrame containing all fields with their properties
- **Triggers**: Materializes when files are detected by the sensor

### 3. `compute_ndvi_raw` Asset
- **Purpose**: Calculates NDVI for each field on each date
- **Input**: 
  - Field geometries from `load_fields`
  - Sentinel-2 satellite imagery via STAC API
- **Processing**:
  - For each (field_id, date) partition combination:
    - Retrieves field geometry and planting date
    - Searches for Sentinel-2 imagery on the specified date
    - Downloads Red (B04) and Near-Infrared (B08) bands
    - Clips bands to field boundary
    - Calculates NDVI: (NIR - RED) / (NIR + RED)
- **Output**: A DataArray containing NDVI values and metadata
- **Partitioning**: Multi-partitioned by field_id and date
- **Triggers**: Materializes for each field after partitions are created

### 4. `daily_ndvi_summary` Asset
- **Purpose**: Aggregates NDVI values across all fields by date
- **Input**: NDVI values from `compute_ndvi_raw`
- **Processing**: Calculates average NDVI for each date
- **Output**: JSON summary stored at `output_data/summary/{date}/daily_summary.json`
- **Partitioning**: Partitioned by date
- **Triggers**: Materializes after NDVI computation for all fields on a date

### 5. `ndvi_anomaly_detector` Asset
- **Purpose**: Detects fields with abnormally low NDVI values
- **Input**: NDVI values from `compute_ndvi_raw`
- **Processing**: Flags fields with NDVI below threshold (0.2)
- **Output**: List of anomaly messages for detected issues
- **Triggers**: Materializes after NDVI computation

### 6. `ndvi_timeseries_report` Asset
- **Purpose**: Generates time series reports for each field
- **Input**: NDVI values from `compute_ndvi_raw`
- **Processing**: Compiles NDVI values over time for each field
- **Output**: CSV report stored at `output_data/timeseries/{field_id}/report.csv`
- **Partitioning**: Partitioned by field_id
- **Triggers**: Materializes after NDVI computation for all dates for a field

### Sensor: `optimized_ndvi_sensor`
- **Purpose**: Monitors MinIO for new or modified input files
- **Input**: S3 bucket `input_data/` directory
- **Processing**:
  - Checks for both bounding_box.geojson and fields.geojson
  - Compares file ETags and timestamps to detect changes
  - Stores hash of current file state
- **Output**: Triggers `ndvi_processing_job` when changes are detected
- **Schedule**: Runs every 60 seconds

## Data Dependencies

The assets form a directed acyclic graph (DAG) with the following dependencies:

```
load_bounding_box ─┐
                   │
load_fields ───────┼──> compute_ndvi_raw ──┬──> daily_ndvi_summary
                   │                       │
                   │                       ├──> ndvi_anomaly_detector
                   │                       │
                   └─────────────────────> └──> ndvi_timeseries_report
```

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