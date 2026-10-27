terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.5.0"
    }
  }
}

provider "google" {
  project = "project-6fed0e0e-2004-4d15-bff"
  region  = "northamerica-northeast1"
}
