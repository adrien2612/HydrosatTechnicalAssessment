terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.27"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }
  }
}

provider "kubernetes" {
  # Assumes KUBECONFIG is set or default ~/.kube/config points to the K3D cluster
  config_path = "~/.kube/config"
  # You might need to specify the context if you have multiple clusters
  # config_context = "k3d-<your-cluster-name>"
}

provider "helm" {
  kubernetes {
    # Assumes KUBECONFIG is set or default ~/.kube/config points to the K3D cluster
    config_path = "~/.kube/config"
    # You might need to specify the context if you have multiple clusters
    # config_context = "k3d-<your-cluster-name>"
  }
} 