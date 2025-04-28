#!/bin/bash
# Dagster NDVI Project Deployment Script

# Set variables
NAMESPACE="dagster"
CLUSTER_NAME="hydrosat-dagster"
IMAGE_NAME="dagster-ndvi:latest"

# Create namespace if it doesn't exist
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

# Create secrets and configmap for dagster
kubectl -n $NAMESPACE apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: minio-creds
type: Opaque
data:
  # echo -n "minio-access-key" | base64
  accessKeyId: bWluaW8tYWNjZXNzLWtleQ==
  # echo -n "minio-secret-key" | base64
  secretAccessKey: bWluaW8tc2VjcmV0LWtleQ==
EOF

kubectl -n $NAMESPACE apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: dagster-config
data:
  DAGSTER_HOME: "/opt/dagster/dagster_home"
  MINIO_ENDPOINT_URL: "http://minio.dagster:9000"
  MINIO_BUCKET_NAME: "dagster-test-bucket"
EOF

# Create dagster.yaml ConfigMap - REQUIRED for pod creation
kubectl -n $NAMESPACE apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: dagster-yaml-config
data:
  dagster.yaml: |
    scheduler:
      module: dagster.core.scheduler
      class: DagsterDaemonScheduler

    run_coordinator:
      module: dagster.core.run_coordinator
      class: QueuedRunCoordinator

    run_launcher:
      module: dagster.core.launcher
      class: DefaultRunLauncher
      
    telemetry:
      enabled: false
EOF

# Build docker image
echo "Building Docker image..."
docker build -t $IMAGE_NAME .

# Import to k3d cluster (if using k3d)
echo "Importing image to k3d cluster $CLUSTER_NAME..."
k3d image import $IMAGE_NAME -c $CLUSTER_NAME

# Delete existing deployments if they exist
echo "Removing existing deployments..."
kubectl -n $NAMESPACE delete deployment dagster-user-deploy dagster-daemon dagit --ignore-not-found

# Deploy Dagster components
echo "Deploying Dagster components..."
kubectl -n $NAMESPACE apply -f k8s/deployment/user-deploy-full.yaml
kubectl -n $NAMESPACE apply -f k8s/deployment/daemon-deploy-full.yaml
kubectl -n $NAMESPACE apply -f k8s/deployment/dagit-deploy-full.yaml

# Create services
echo "Creating or updating services..."
kubectl -n $NAMESPACE apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: dagster-grpc
  labels:
    app: dagster-grpc
spec:
  selector:
    app: dagster-user-deploy
  ports:
  - port: 4000
    targetPort: 4000
EOF

kubectl -n $NAMESPACE apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: dagit
  labels:
    app: dagit
spec:
  selector:
    app: dagit
  ports:
  - port: 80
    targetPort: 3000
EOF

echo "Deployment completed!"
echo "Dagit is deployed at http://localhost:3000 (after port-forwarding)"
echo "To access the UI, run: kubectl -n $NAMESPACE port-forward svc/dagit 3000:80"

# Wait for the dagit pod to be ready
echo "Waiting for Dagit pod to be ready..."
kubectl -n $NAMESPACE wait --for=condition=ready pod -l app=dagit --timeout=120s

# Check that all pods are running before port-forwarding
echo "Checking pod status..."
kubectl -n $NAMESPACE get pods

# Now set up port-forwarding
echo "Setting up port-forwarding..."
kubectl -n $NAMESPACE port-forward svc/dagit 3000:80 