# Independent acceptance state: never operate on the production Terraform root.
terraform {
  backend "gcs" {
    bucket = "transit-realtime-tfstate-26"
    prefix = "transit-realtime-pipeline/verification/gcs-repeat"
  }
}
