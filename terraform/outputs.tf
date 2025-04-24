output "minio_endpoint_url" {
  description = "The endpoint URL template for the MinIO Console UI service. The setup script provides the final localhost URL."
  value       = "http://<NODE_IP>:30900" # Use localhost:30900 when accessed via k3d load balancer
}

output "minio_access_key" {
  description = "Access key for MinIO"
  value       = var.minio_access_key
  sensitive   = true
}

output "minio_secret_key" {
  description = "Secret key for MinIO"
  value       = var.minio_secret_key
  sensitive   = true
}

output "minio_bucket_name" {
  description = "Name of the bucket created for Dagster data"
  value       = var.dagster_data_bucket
}

output "kubernetes_namespace" {
  description = "Namespace where resources are deployed"
  value       = kubernetes_namespace.dagster_minio.metadata[0].name
} 