terraform {
  backend "gcs" {
    bucket = "transit-realtime-tfstate-26"
    prefix = "transit-realtime-pipeline/databricks-access/dev"
  }
}
