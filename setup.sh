#!/bin/bash

# Exit script on any error
set -e

# Configuration
CLUSTER_NAME="hydrosat-dagster"
KUBE_CONTEXT="k3d-${CLUSTER_NAME}"
MINIO_PORT="9001"
MINIO_API_PORT="9000"
MINIO_CONTAINER_NAME="minio-local"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
DAGSTER_DATA_BUCKET="dagster-data"

echo "--- Checking Prerequisites ---"

# Check for docker
if ! command -v docker &> /dev/null
then
    echo "Error: docker command could not be found. Please install Docker: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check for k3d
if ! command -v k3d &> /dev/null
then
    echo "Error: k3d command could not be found. Please install k3d: https://k3d.io/#installation"
    exit 1
fi

# Check for kubectl
if ! command -v kubectl &> /dev/null
then
    echo "Error: kubectl command could not be found. Please install kubectl: https://kubernetes.io/docs/tasks/tools/install-kubectl/"
    exit 1
fi

echo "--- Prerequisites Met ---"

echo -e "\n--- Setting Up MinIO via Docker ---"

# Check if the MinIO container is already running
if [ "$(docker ps -q -f name=${MINIO_CONTAINER_NAME})" ]; then
    echo "MinIO container '${MINIO_CONTAINER_NAME}' already exists and is running."
else
    # Check if the container exists but is stopped
    if [ "$(docker ps -aq -f status=exited -f name=${MINIO_CONTAINER_NAME})" ]; then
        echo "Found stopped MinIO container. Removing it..."
        docker rm ${MINIO_CONTAINER_NAME}
    fi

    echo "Starting MinIO container..."
    docker run -d \
      --name ${MINIO_CONTAINER_NAME} \
      -p ${MINIO_PORT}:9001 \
      -p ${MINIO_API_PORT}:9000 \
      -e "MINIO_ROOT_USER=${MINIO_ACCESS_KEY}" \
      -e "MINIO_ROOT_PASSWORD=${MINIO_SECRET_KEY}" \
      --health-cmd "curl -f http://localhost:9000/minio/health/live || exit 1" \
      --health-interval 5s \
      --health-timeout 2s \
      --health-retries 3 \
      quay.io/minio/minio:RELEASE.2023-11-15T20-43-25Z server /data --console-address ":9001"
fi

# Wait for MinIO to be healthy
echo "Waiting for MinIO to be ready..."
max_retries=30
counter=0
while [ $counter -lt $max_retries ]; do
    if docker inspect --format='{{.State.Health.Status}}' ${MINIO_CONTAINER_NAME} 2>/dev/null | grep -q "healthy"; then
        echo "MinIO is ready!"
        break
    fi
    echo -n "."
    sleep 1
    counter=$((counter+1))
done

if [ $counter -eq $max_retries ]; then
    echo "Failed to start MinIO container in a timely manner. Please check 'docker logs ${MINIO_CONTAINER_NAME}'."
    exit 1
fi

# Create the bucket if it doesn't exist
echo -e "\n--- Creating MinIO Bucket: ${DAGSTER_DATA_BUCKET} ---"
docker exec ${MINIO_CONTAINER_NAME} bash -c "mkdir -p /data/${DAGSTER_DATA_BUCKET}/input_data /data/${DAGSTER_DATA_BUCKET}/output_data"

# Create input_data and processed_data subdirectories
echo "Creating directory structure for input_data and processed_data..."
docker exec ${MINIO_CONTAINER_NAME} bash -c "mkdir -p /data/${DAGSTER_DATA_BUCKET}/input_data/processed_data"

echo -e "\n--- Setting Up K3D Cluster ---"

# Check if cluster already exists
if k3d cluster list | grep -q "^${CLUSTER_NAME}"; then
    echo "K3D cluster '${CLUSTER_NAME}' already exists. Skipping creation."
else
    echo "Creating K3D cluster..."
    k3d cluster create "${CLUSTER_NAME}" --agents 1
    sleep 5 # Give it a moment to initialize
    kubectl wait --for=condition=Ready nodes --all --timeout=60s
fi

echo -e "\n--- Setting Kubectl Context to ${KUBE_CONTEXT} ---"
kubectl config use-context "${KUBE_CONTEXT}"

echo -e "\n--- Verifying Cluster Connection ---"
kubectl cluster-info

echo -e "\n--- Creating 'dagster' Namespace ---"
kubectl create namespace dagster --dry-run=client -o yaml | kubectl apply -f -

echo -e "\n--- Infrastructure Setup Complete! ---"

# Calculate host IP for connecting to MinIO from K8s
# On Mac, use host.docker.internal; on Linux use the docker0 interface IP
if [[ "$OSTYPE" == "darwin"* ]]; then
    HOST_IP="host.docker.internal"
else
    # Get IP of docker0 interface on Linux
    HOST_IP=$(ip -4 addr show docker0 | grep -oP '(?<=inet\s)\d+(\.\d+){3}')
fi

echo -e "\n--- Access Details ---"
echo "MinIO Console URL: http://localhost:${MINIO_PORT}"
echo "MinIO API Endpoint (for Dagster): http://${HOST_IP}:${MINIO_API_PORT}"
echo "MinIO Access Key: ${MINIO_ACCESS_KEY}"
echo "MinIO Secret Key: ${MINIO_SECRET_KEY}"
echo "MinIO Bucket: ${DAGSTER_DATA_BUCKET}"

echo -e "\nNext Steps:"
echo "1. Open the MinIO Console URL in your browser: http://localhost:${MINIO_PORT}"
echo "2. Log in using the Access Key: ${MINIO_ACCESS_KEY} and Secret Key: ${MINIO_SECRET_KEY}"
echo "3. Configure Dagster to use MinIO with the following details:"
echo "   - S3 Endpoint URL: http://${HOST_IP}:${MINIO_API_PORT}"
echo "   - Access Key: ${MINIO_ACCESS_KEY}"
echo "   - Secret Key: ${MINIO_SECRET_KEY}"
echo "   - Bucket Name: ${DAGSTER_DATA_BUCKET}"
echo "4. Proceed with building and deploying Dagster to the K3D cluster." 