terraform {
  required_version = ">= 1.6.0"

  cloud {
    organization = "pkai21_organ"

    workspaces {
      name = "california-housing-monitoring"
    }
  }

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }

    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }
}