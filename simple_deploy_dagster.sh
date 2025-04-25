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
      config:
        max_concurrent_runs: 3
        dequeue_interval_seconds: 30
    
    run_launcher:
      module: dagster_k8s.launcher
      class: K8sRunLauncher
      config:
        job_namespace: ${NAMESPACE}
        image_pull_policy: Never
        instance_config_map: dagster-instance-yaml
        service_account_name: default
        job_image: dagster-ndvi:latest
        run_k8s_config:
          container_config:
            resources:
              requests:
                memory: "1Gi"
                cpu: "500m"
              limits:
                memory: "2Gi"
                cpu: "1000m"
            volume_mounts:
              - name: dagster-home
                mountPath: /opt/dagster/dagster_home
              - name: dagster-instance
                mountPath: /opt/dagster/dagster_home/dagster.yaml
                subPath: dagster.yaml
            env:
              - name: MINIO_ENDPOINT_URL
                valueFrom:
                  configMapKeyRef:
                    name: dagster-config
                    key: MINIO_ENDPOINT_URL
              - name: MINIO_ACCESS_KEY
                valueFrom:
                  secretKeyRef:
                    name: dagster-secrets
                    key: MINIO_ACCESS_KEY
              - name: MINIO_SECRET_KEY
                valueFrom:
                  secretKeyRef:
                    name: dagster-secrets
                    key: MINIO_SECRET_KEY
              - name: MINIO_BUCKET_NAME
                valueFrom:
                  configMapKeyRef:
                    name: dagster-config
                    key: MINIO_BUCKET_NAME
          pod_template_spec_metadata:
            labels:
              app: dagster-run
          pod_spec_config:
            volumes:
              - name: dagster-home
                emptyDir: {}
              - name: dagster-instance
                configMap:
                  name: dagster-instance-yaml
EOF

echo "--- Creating RBAC for Dagster ---"
kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: dagster-job-role
  namespace: ${NAMESPACE}
rules:
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["get", "watch", "list", "create", "delete", "patch"]
- apiGroups: [""]
  resources: ["pods", "pods/log"]
  verbs: ["get", "watch", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: dagster-job-binding
  namespace: ${NAMESPACE}
subjects:
- kind: ServiceAccount
  name: default
  namespace: ${NAMESPACE}
roleRef:
  kind: Role
  name: dagster-job-role
  apiGroup: rbac.authorization.k8s.io
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
        - name: DAGSTER_GRPC_STARTUP_TIMEOUT
          value: "120"  # Increased timeout for GRPC startup
        - name: PYTHONUNBUFFERED
          value: "1"    # Ensure Python output is unbuffered for better logs
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
            memory: "1.5Gi"
            cpu: "500m"
          limits:
            memory: "2.5Gi"
            cpu: "1000m"
        readinessProbe:
          tcpSocket:
            port: 4000
          initialDelaySeconds: 20
          periodSeconds: 10
          failureThreshold: 5
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
        - name: PYTHONUNBUFFERED
          value: "1"
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
            memory: "1.5Gi"
            cpu: "500m"
          limits:
            memory: "2.5Gi"
            cpu: "1000m"
        livenessProbe:
          exec:
            command:
            - sh
            - -c
            - ps -ef | grep "dagster-daemon run" | grep -v grep
          initialDelaySeconds: 60
          periodSeconds: 20
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
        - name: PYTHONUNBUFFERED
          value: "1"
        - name: DAGSTER_WEBSERVER_TIMEOUT
          value: "60"
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
            memory: "1.5Gi"
            cpu: "500m"
          limits:
            memory: "2.5Gi"
            cpu: "1000m"
        startupProbe:
          httpGet:
            path: /healthz
            port: 3000
          failureThreshold: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /healthz
            port: 3000
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
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

echo "--- Waiting an additional 30 seconds for services to initialize properly ---"
sleep 30

echo "--- Starting port forwarding with retry mechanism ---"
PORT_FORWARD_SUCCESS=false
PORT_FORWARD_PID=""
MAX_RETRIES=5
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ] && [ "$PORT_FORWARD_SUCCESS" = false ]; do
  RETRY_COUNT=$((RETRY_COUNT+1))
  echo "Attempt $RETRY_COUNT of $MAX_RETRIES to start port forwarding..."
  
  # Check if webserver pod is ready first
  if kubectl -n $NAMESPACE get pods -l app=dagster-webserver | grep -q "Running"; then
    kubectl port-forward -n $NAMESPACE svc/dagster-webserver 8080:3000 &
    PORT_FORWARD_PID=$!
    
    # Give port forwarding a moment to establish
    sleep 5
    
    # Check if port forwarding is working
    if ps -p $PORT_FORWARD_PID > /dev/null && curl -s http://localhost:8080 > /dev/null; then
      PORT_FORWARD_SUCCESS=true
      echo "Port forwarding successfully established!"
    else
      echo "Port forwarding failed. Killing process and retrying..."
      kill $PORT_FORWARD_PID 2>/dev/null || true
      sleep 10
    fi
  else
    echo "Webserver pod not yet in Running state. Waiting before retry..."
    sleep 15
  fi
done

if [ "$PORT_FORWARD_SUCCESS" = false ]; then
  echo "WARNING: Automatic port forwarding failed after $MAX_RETRIES attempts."
  echo "You may need to manually run: kubectl port-forward -n $NAMESPACE svc/dagster-webserver 8080:3000"
else
  echo "--- Deployment Complete! ---"
  echo "You can access the Dagster UI at: http://localhost:8080"
  echo "To check pod status, run: kubectl get pods -n ${NAMESPACE}"
  echo ""
  echo "Port forwarding is running in the background (PID: $PORT_FORWARD_PID)"
  echo "To stop port forwarding: kill $PORT_FORWARD_PID" 
fi 