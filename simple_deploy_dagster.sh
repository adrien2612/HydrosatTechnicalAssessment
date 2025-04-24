#!/bin/bash

# Exit script on any error
set -e

# Configuration
NAMESPACE="dagster"
K8S_CONTEXT="k3d-hydrosat-dagster"
MINIO_ENDPOINT_URL="http://host.docker.internal:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
MINIO_BUCKET_NAME="dagster-data"

echo "--- Checking Prerequisites ---"
# Check required commands
for cmd in docker kubectl; do
    if ! command -v $cmd &> /dev/null; then
        echo "Error: $cmd command could not be found"
        exit 1
    fi
done

echo "--- Setting kubectl context to $K8S_CONTEXT ---"
kubectl config use-context "$K8S_CONTEXT"

# Creating namespace if it doesn't exist
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

echo "--- Building Dagster Docker Image ---"
cd dagster_ndvi_project
docker build -t dagster-ndvi:latest .
echo "--- Loading Docker Image into K3D ---"
k3d image import dagster-ndvi:latest -c hydrosat-dagster
cd ..

echo "--- Creating Dagster ConfigMap and Secret ---"
kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: dagster-config
  namespace: ${NAMESPACE}
data:
  MINIO_ENDPOINT_URL: "${MINIO_ENDPOINT_URL}"
  MINIO_BUCKET_NAME: "${MINIO_BUCKET_NAME}"
EOF

kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: dagster-secrets
  namespace: ${NAMESPACE}
type: Opaque
stringData:
  MINIO_ACCESS_KEY: "${MINIO_ACCESS_KEY}"
  MINIO_SECRET_KEY: "${MINIO_SECRET_KEY}"
EOF

echo "--- Creating Dagster Workspace ConfigMap ---"
kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: workspace-config
  namespace: ${NAMESPACE}
data:
  workspace.yaml: |
    load_from:
      - grpc_server:
          host: dagster-code
          port: 4000
          location_name: "dagster-ndvi-project"
EOF

echo "--- Creating Dagster Home Directory Structure ---"
kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: dagster-instance-yaml
  namespace: ${NAMESPACE}
data:
  dagster.yaml: |
    telemetry:
      enabled: false
    
    scheduler:
      module: dagster.core.scheduler
      class: DagsterDaemonScheduler
    
    run_coordinator:
      module: dagster.core.run_coordinator
      class: QueuedRunCoordinator
    
    run_launcher:
      module: dagster_k8s.launcher
      class: K8sRunLauncher
      config:
        job_namespace: ${NAMESPACE}
        image_pull_policy: Never
EOF

echo "--- Deploying Dagster Code Server ---"
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dagster-code
  namespace: ${NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dagster-code
  template:
    metadata:
      labels:
        app: dagster-code
    spec:
      containers:
      - name: dagster-code
        image: dagster-ndvi:latest
        imagePullPolicy: Never
        command: ["/entrypoint.sh", "grpc"]
        ports:
        - containerPort: 4000
        env:
        - name: DAGSTER_HOME
          value: "/opt/dagster/dagster_home"
        envFrom:
        - configMapRef:
            name: dagster-config
        - secretRef:
            name: dagster-secrets
        volumeMounts:
        - name: dagster-home
          mountPath: /opt/dagster/dagster_home
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
      volumes:
      - name: dagster-home
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: dagster-code
  namespace: ${NAMESPACE}
spec:
  selector:
    app: dagster-code
  ports:
  - port: 4000
    targetPort: 4000
EOF

echo "--- Deploying Dagster Daemon ---"
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dagster-daemon
  namespace: ${NAMESPACE}
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
      - name: dagster-daemon
        image: dagster-ndvi:latest
        imagePullPolicy: Never
        command: ["dagster-daemon", "run"]
        env:
        - name: DAGSTER_HOME
          value: "/opt/dagster/dagster_home"
        envFrom:
        - configMapRef:
            name: dagster-config
        - secretRef:
            name: dagster-secrets
        volumeMounts:
        - name: dagster-home
          mountPath: /opt/dagster/dagster_home
        - name: dagster-instance
          mountPath: /opt/dagster/dagster_home/dagster.yaml
          subPath: dagster.yaml
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
      volumes:
      - name: dagster-home
        emptyDir: {}
      - name: dagster-instance
        configMap:
          name: dagster-instance-yaml
EOF

echo "--- Deploying Dagster Webserver ---"
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dagster-webserver
  namespace: ${NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dagster-webserver
  template:
    metadata:
      labels:
        app: dagster-webserver
    spec:
      containers:
      - name: dagster-webserver
        image: dagster-ndvi:latest
        imagePullPolicy: Never
        command: ["dagster-webserver"]
        args: ["-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/app/workspace/workspace.yaml"]
        ports:
        - containerPort: 3000
        env:
        - name: DAGSTER_HOME
          value: "/opt/dagster/dagster_home"
        envFrom:
        - configMapRef:
            name: dagster-config
        - secretRef:
            name: dagster-secrets
        volumeMounts:
        - name: dagster-home
          mountPath: /opt/dagster/dagster_home
        - name: dagster-instance
          mountPath: /opt/dagster/dagster_home/dagster.yaml
          subPath: dagster.yaml
        - name: workspace-config
          mountPath: /opt/dagster/app/workspace/workspace.yaml
          subPath: workspace.yaml
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
      volumes:
      - name: dagster-home
        emptyDir: {}
      - name: dagster-instance
        configMap:
          name: dagster-instance-yaml
      - name: workspace-config
        configMap:
          name: workspace-config
---
apiVersion: v1
kind: Service
metadata:
  name: dagster-webserver
  namespace: ${NAMESPACE}
spec:
  selector:
    app: dagster-webserver
  ports:
  - port: 3000
    targetPort: 3000
    nodePort: 30300
  type: NodePort
EOF

# Delete test pod if it exists
kubectl delete pod dagster-test -n ${NAMESPACE} --ignore-not-found=true

echo "--- Waiting for all pods to be ready ---"
sleep 5  # Give time for old pods to terminate
kubectl -n $NAMESPACE wait --for=condition=Ready pods --all --timeout=300s || true

echo "--- Starting port forwarding in the background ---"
kubectl port-forward -n $NAMESPACE svc/dagster-webserver 8080:3000 &
PORT_FORWARD_PID=$!

echo "--- Deployment Complete! ---"
echo "You can access the Dagster UI at: http://localhost:8080"
echo "To check pod status, run: kubectl get pods -n ${NAMESPACE}"
echo ""
echo "Port forwarding is running in the background (PID: $PORT_FORWARD_PID)"
echo "To stop port forwarding: kill $PORT_FORWARD_PID" 