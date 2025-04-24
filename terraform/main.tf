resource "kubernetes_namespace" "dagster_minio" {
  metadata {
    name = var.namespace
  }
}

# Comment out or remove the Helm release
# resource "helm_release" "minio" {
#   name       = "minio"
#   repository = "https://charts.min.io/"
#   chart      = "minio"
#   version    = var.minio_chart_version
#   namespace  = kubernetes_namespace.dagster_minio.metadata[0].name
#   wait       = false # Added previously, but we are removing the whole block
#
#   set {
#     name  = "accessKey"
#     value = var.minio_access_key
#   }
#
#   set {
#     name  = "secretKey"
#     value = var.minio_secret_key
#   }
#
#   set {
#     name  = "service.type"
#     value = "NodePort" # Easier access for local K3D setup
#   }
#
#   set {
#     name  = "service.port"
#     value = "9001" # Target the Console port (9001)
#   }
#
#   set {
#     name  = "service.nodePort"
#     value = "30900" # Fixed nodeport for predictable access
#   }
#
#   set {
#     name = "mode"
#     value = "standalone" # Simple single-node setup for K3D
#   }
#
#   set {
#     name  = "persistence.enabled"
#     value = "false" # Disable persistence for simplicity
#   }
#
#   set {
#     name  = "resources.requests.memory"
#     value = "512Mi" # Set a reasonable memory request
#   }
#
#   # Bucket creation commented out previously
#
#   depends_on = [kubernetes_namespace.dagster_minio]
# }

# Define MinIO Deployment directly
resource "kubernetes_deployment" "minio" {
  metadata {
    name      = "minio"
    namespace = kubernetes_namespace.dagster_minio.metadata[0].name
    labels = {
      app = "minio"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "minio"
      }
    }

    template {
      metadata {
        labels = {
          app = "minio"
        }
      }

      spec {
        container {
          name  = "minio"
          image = "quay.io/minio/minio:RELEASE.2024-07-19T18-11-50Z" # Corrected image tag
          args  = ["server", "/data", "--console-address", ":9001"]

          port {
            name           = "s3-api"
            container_port = 9000
          }
          port {
            name           = "console"
            container_port = 9001
          }

          env {
            name  = "MINIO_ROOT_USER"
            value = var.minio_access_key
          }
          env {
            name  = "MINIO_ROOT_PASSWORD"
            value = var.minio_secret_key
          }

          # Define a simple readiness probe targeting the API port
          readiness_probe {
            http_get {
              path = "/minio/health/ready"
              port = 9000
            }
            initial_delay_seconds = 30
            period_seconds      = 15
            timeout_seconds     = 5
            failure_threshold   = 6
            success_threshold   = 1
          }

          # Define a simple liveness probe targeting the API port
          liveness_probe {
            http_get {
              path = "/minio/health/live"
              port = 9000
            }
            initial_delay_seconds = 60
            period_seconds      = 20
            timeout_seconds     = 5
            failure_threshold   = 5
            success_threshold   = 1
          }

          # Optional: Set resource requests/limits (adjust as needed)
          resources {
            requests = {
              memory = "256Mi"
              cpu    = "100m"
            }
            limits = {
              memory = "1Gi"
              cpu    = "500m"
            }
          }

          # No volume mounts for persistence to keep it simple
        }
      }
    }
  }
  depends_on = [kubernetes_namespace.dagster_minio]
}

# Define MinIO Service directly
resource "kubernetes_service" "minio" {
  metadata {
    name      = "minio"
    namespace = kubernetes_namespace.dagster_minio.metadata[0].name
  }
  spec {
    selector = {
      app = "minio"
    }
    port {
      name        = "minio-console"
      port        = 9001 # Service port (target for internal cluster comms if needed)
      target_port = 9001 # Pod's console port
      node_port   = 30900 # Expose on host via K3D load balancer
    }
    # We also expose the API port for internal cluster communication (e.g., for Dagster)
    port {
        name = "minio-api"
        port = 9000        # Service port
        target_port = 9000 # Pod's API port
    }

    type = "NodePort"
  }
  depends_on = [kubernetes_deployment.minio]
} 