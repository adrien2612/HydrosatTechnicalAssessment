variable "cluster_name" {
  description = "Name of the K3D cluster"
  type        = string
  default     = "hydrosat-dagster"
}

variable "namespace" {
  description = "Kubernetes namespace for deployment"
  type        = string
  default     = "dagster-minio"
}

variable "minio_access_key" {
  description = "Access key for MinIO"
  type        = string
  default     = "minioadmin"
  sensitive   = true
}

variable "minio_secret_key" {
  description = "Secret key for MinIO"
  type        = string
  default     = "minioadmin"
  sensitive   = true
}

variable "minio_chart_version" {
  description = "Version of the MinIO Helm chart"
  type        = string
  default     = "5.0.10" # Use a recent, stable version
}

variable "dagster_data_bucket" {
  description = "Name of the S3 bucket for Dagster inputs/outputs"
  type        = string
  default     = "dagster-data"
} 