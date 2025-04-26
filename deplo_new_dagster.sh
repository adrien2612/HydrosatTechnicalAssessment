#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------
# Shell script to deploy Dagster to k3d
# Assumes:
#  • You already have a k3d cluster running and kubectl context set
#  • You have an external MinIO (outside the cluster)
#  • You have pushed your Dagster image to a local registry or will load it
# ---------------------------------------

# --- Configuration (edit these) ---
CLUSTER_NAME="hydrosat-dagster"                    # your k3d cluster name
NAMESPACE="dagster"
IMAGE_NAME="dagster-ndvi:latest"  # Use just the image name, we'll load it directly into k3d
MINIO_ENDPOINT_URL=http://host.docker.internal:9000
MINIO_BUCKET_NAME="dagster-data"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"

# --- 1) Ensure namespace exists ---
if ! kubectl get namespace "$NAMESPACE" &>/dev/null; then
  kubectl create namespace "$NAMESPACE"
fi

# --- 2) Create/update MinIO creds secret ---
kubectl -n "$NAMESPACE" create secret generic minio-creds \
  --from-literal=MINIO_ACCESS_KEY="$MINIO_ACCESS_KEY" \
  --from-literal=MINIO_SECRET_KEY="$MINIO_SECRET_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -

# --- 3) Create/update Dagster configmap ---
kubectl -n "$NAMESPACE" create configmap dagster-config \
  --from-literal=MINIO_ENDPOINT_URL="$MINIO_ENDPOINT_URL" \
  --from-literal=MINIO_BUCKET_NAME="$MINIO_BUCKET_NAME" \
  --from-literal=DAGSTER_HOME="/opt/dagster/dagster_home" \
  --dry-run=client -o yaml | kubectl apply -f -

# --- 4) Build & import your Dagster image into k3d ---
docker build -t "$IMAGE_NAME" .
k3d image import "$IMAGE_NAME" -c "$CLUSTER_NAME"

# --- 5) Deploy Dagster components ---
kubectl -n "$NAMESPACE" apply -f - <<EOF
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dagster-user-deploy
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dagster-user-deploy
  template:
    metadata:
      labels:
        app: dagster-user-deploy
    spec:
      containers:
      - name: user-deploy
        imagePullPolicy: Never
        image: $IMAGE_NAME
        command: ["/entrypoint.sh"]
        args: ["grpc"]
        envFrom:
        - configMapRef:
            name: dagster-config
        - secretRef:
            name: minio-creds
        env:
        - name: DAGSTER_HOME
          value: "/opt/dagster/dagster_home"
        ports:
        - containerPort: 4000
        volumeMounts:
        - name: dagster-home
          mountPath: /opt/dagster/dagster_home
        resources:
          requests:
            memory: "512Mi"
          limits:
            memory: "1Gi"
      volumes:
      - name: dagster-home
        emptyDir: {}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dagster-daemon
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dagster-daemon
  template:
    metadata:
      labels:
        app: dagster-daemon
    spec:
      containers:
      - name: daemon
        imagePullPolicy: Never
        image: $IMAGE_NAME
        command: ["/entrypoint.sh"]
        args: ["daemon"]
        envFrom:
        - configMapRef:
            name: dagster-config
        - secretRef:
            name: minio-creds
        env:
        - name: DAGSTER_HOME
          value: "/opt/dagster/dagster_home"
        volumeMounts:
        - name: dagster-home
          mountPath: /opt/dagster/dagster_home
        resources:
          requests:
            memory: "512Mi"
          limits:
            memory: "1Gi"
      volumes:
      - name: dagster-home
        emptyDir: {}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dagit
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dagit
  template:
    metadata:
      labels:
        app: dagit
    spec:
      containers:
      - name: dagit
        imagePullPolicy: Never
        image: $IMAGE_NAME
        command: ["/entrypoint.sh"]
        args: ["dev"]
        envFrom:
        - configMapRef:
            name: dagster-config
        - secretRef:
            name: minio-creds
        env:
        - name: DAGSTER_HOME
          value: "/opt/dagster/dagster_home"
        ports:
        - containerPort: 3000
        volumeMounts:
        - name: dagster-home
          mountPath: /opt/dagster/dagster_home
      volumes:
      - name: dagster-home
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: dagster-grpc
spec:
  selector:
    app: dagster-user-deploy
  ports:
  - port: 4000
    targetPort: 4000
---
apiVersion: v1
kind: Service
metadata:
  name: dagit
spec:
  selector:
    app: dagit
  ports:
  - port: 80
    targetPort: 3000
EOF

# --- 6) (Optional) Port-forward to access Dagit & gRPC locally ---
echo "Dagit deployed at http://localhost:3000:80"
echo "Waiting for Dagster to be ready..."
sleep 60
kubectl port-forward -n $NAMESPACE svc/dagit 3000:80
echo "To access Dagster gRPC API, run:"
echo "  kubectl port-forward -n $NAMESPACE svc/dagster-grpc 4000:4000"

echo "✅ Dagster deployed to namespace '$NAMESPACE' on your k3d cluster."
