terraform {
  required_version = ">= 1.6.0, < 2.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "8.5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "1.137.0"
    }
  }
}

locals {
  project_id     = "project-6fed0e0e-2004-4d15-bff"
  landing_bucket = "transit-realtime-landing-26"
  metastore_id   = "6f5c957c-823b-41cc-be85-2573bd074b5b"
}

provider "google" {
  project = local.project_id
  region  = "northamerica-northeast1"
}

# Use the existing CLI login; no token or service-account key in configuration.
provider "databricks" {
  host      = "https://8259562609571268.8.gcp.databricks.com"
  profile   = "transit-dev"
  auth_type = "databricks-cli"
}
