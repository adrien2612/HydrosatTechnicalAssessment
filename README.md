# Dagster NDVI Pipeline Project

This project implements a Normalized Difference Vegetation Index (NDVI) processing pipeline using Dagster on Kubernetes. The pipeline is configured to process geospatial data from MinIO storage.

## Project Structure

- `dagster_ndvi_project/` - Main Dagster code
  - `assets.py` - Asset definitions (NDVI processing logic)
  - `sensors.py` - Sensors for detecting new input data
  - `resources.py` - Resource definitions (MinIO connectivity)
  - `definitions.py` - Dagster definitions
- `helm-dagster-values.yaml` - Helm chart values for Dagster deployment
- `sample_fields.geojson` - Sample input data for testing
- `upload_test_data.py` - Script to upload test data to MinIO
- `test_utils.sh` - Helper functions for testing

## Requirements

- Kubernetes cluster (local or remote)
- Helm
- kubectl
- Python 3.8+
- MinIO instance (running in Kubernetes or externally)

## Setup Instructions

### 1. Deploy MinIO to Kubernetes

```bash
# Add MinIO Helm repository
helm repo add minio https://charts.min.io/

# Install MinIO in the dagster namespace
helm install minio minio/minio \
  --namespace dagster \
  --create-namespace \
  --set accessKey=minioadmin \
  --set secretKey=minioadmin \
  --set persistence.enabled=false \
  --set service.type=ClusterIP
```

### 2. Build and Deploy the Dagster NDVI Project

```bash
# Build the Docker image
docker build -t my-dagster-ndvi-project:latest .

# Deploy Dagster with Helm
helm repo add dagster https://dagster-io.github.io/helm
helm repo update

# Install Dagster with our custom values
helm install dagster dagster/dagster \
  --namespace dagster \
  --create-namespace \
  -f helm-dagster-values.yaml
```

### 3. Upload Test Data

```bash
# Source the helper functions
source test_utils.sh

# Upload test data to MinIO
upload_test_data
```

### 4. Access the Dagster UI

```bash
# Port-forward the Dagster webserver
kubectl port-forward -n dagster svc/dagster-webserver 3000:80
```

Visit http://localhost:3000 in your browser.

## Testing the Pipeline

The project includes a simple test asset (`simple_field_id`) and a simplified sensor (`ndvi_simple_sensor`) to test the pipeline functionality with minimal dependencies.

### Running Test Utils

```bash
# Source the test utilities
source test_utils.sh

# See available functions
show_help

# Check Dagster deployment status
check_dagster_status

# List recent runs
list_runs

# Check logs for a specific run
check_run_logs <run_id>
```

## Configuring the Dagster Instance

The Dagster instance is configured to limit the maximum number of concurrent runs to 1 using the `QueuedRunCoordinator` with `maxConcurrentRuns: 1`. This configuration is defined in the `helm-dagster-values.yaml` file.

To apply changes to the instance configuration:

```bash
# Source the test utilities
source test_utils.sh

# Apply the instance configuration
apply_instance_config
```

## Debugging

The project includes extensive logging in both the sensor and asset code to help diagnose issues. You can check the logs of the Dagster daemon and run pods:

```bash
# Check Dagster daemon logs
kubectl logs -n dagster $(kubectl get pods -n dagster -l app=dagster-daemon -o jsonpath='{.items[0].metadata.name}')

# Check logs for a specific run
kubectl logs -n dagster <run-pod-name>
```

## MinIO Configuration

The MinIO configuration is defined in environment variables:

- `MINIO_ENDPOINT_URL` - MinIO endpoint URL
- `MINIO_ACCESS_KEY` - MinIO access key
- `MINIO_SECRET_KEY` - MinIO secret key
- `MINIO_BUCKET_NAME` - MinIO bucket name

These variables are set in the Helm values file and passed to all Dagster components. 