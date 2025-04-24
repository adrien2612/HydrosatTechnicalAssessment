# Dagster NDVI Processing on Kubernetes

This project provides a Dagster-based workflow for processing NDVI (Normalized Difference Vegetation Index) satellite data on Kubernetes. The implementation follows best practices for running data pipelines in production.

## Architecture

This deployment uses:

1. **Dagster on Kubernetes** - For workflow orchestration with parallel execution
2. **MinIO** - As S3-compatible object storage for input/output data
3. **PostgreSQL** - For storing Dagster metadata and run history
4. **Helm** - For simplified deployment of the entire stack

## Key Features

- **Optimized Docker Build** - Multi-stage build to speed up container creation
- **Kubernetes Job Executor** - Parallel execution of tasks across Kubernetes pods
- **Resource Management** - CPU and memory limits for all components
- **S3-compatible Storage** - MinIO for efficient data handling
- **Automatic Sensors** - Watch for new data and trigger processing

## Prerequisites

- Docker
- Kubernetes cluster (K3d/Kind for local development, EKS/GKE/AKS for production)
- Helm 3
- kubectl

## Getting Started

### Step 1: Setting up the Development Environment

Run the setup script to create a local development environment:

```bash
./setup.sh
```

This creates:
- A K3d Kubernetes cluster
- A MinIO instance for S3-compatible storage
- Required buckets and directories

### Step 2: Deploy Dagster to Kubernetes

Deploy Dagster using the optimized Helm-based deployment:

```bash
./deploy_dagster.sh
```

This script:
1. Builds an optimized Docker image for Dagster
2. Loads the image into your Kubernetes cluster
3. Deploys Dagster using Helm with appropriate configurations
4. Sets up all required environment variables

### Step 3: Access the Dagster UI

Once deployed, access the Dagster UI:

```bash
# Option 1: Direct access via NodePort
http://localhost:30080

# Option 2: Port forwarding
kubectl port-forward -n dagster svc/dagster-dagster-webserver 3000:80
http://localhost:3000
```

## Project Structure

- `dagster_ndvi_project/` - Main Dagster project
  - `dagster_ndvi_project/assets.py` - NDVI processing asset definitions
  - `dagster_ndvi_project/resources.py` - MinIO and other resource configurations
  - `dagster_ndvi_project/sensors.py` - Sensors for monitoring new data
  - `dagster_ndvi_project/definitions.py` - Main Dagster definitions
- `setup.sh` - Environment setup script
- `deploy_dagster.sh` - Helm-based Dagster deployment script

## Advanced Configuration

### Scaling for Production

For production environments, consider:

1. **External PostgreSQL**: Configure an external managed PostgreSQL database
   ```yaml
   postgresql:
     enabled: false
     postgresqlHost: 'your-postgres-host'
     postgresqlUsername: 'your-username'
     postgresqlPassword: 'your-password'
     postgresqlDatabase: 'dagster'
   ```

2. **Increase Resources**: Adjust CPU/memory in the Helm values file
   ```yaml
   dagsterWebserver:
     resources:
       requests:
         cpu: 1
         memory: 1Gi
       limits:
         cpu: 2
         memory: 2Gi
   ```

3. **Use Celery for Resource Limiting**: For more complex queue management
   ```yaml
   runLauncher:
     type: CeleryK8sRunLauncher
     config:
       celeryK8sRunLauncher:
         workerQueues:
           - name: default
             replicaCount: 5
   ```

### Running on Different Cloud Providers

Detailed instructions for AWS, GCP, and Azure are provided in their respective README sections.

## Troubleshooting

Common issues and solutions:

1. **Pod Crashes**: Check logs with `kubectl logs -n dagster <pod-name>`
2. **Slow Builds**: Consider using pre-built wheels for geospatial libraries
3. **MinIO Connectivity**: Ensure proper endpoint URLs for your environment

## License

MIT 