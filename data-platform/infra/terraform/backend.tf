terraform {
  required_version = ">= 1.6.0, < 2.0.0"

  backend "gcs" {
    bucket = "transit-realtime-tfstate-26"
    prefix = "transit-realtime-pipeline/dev"
  }
}
