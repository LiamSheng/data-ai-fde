# Showcase only (branch `showcase`). Its own state, so `terraform destroy` here removes
# everything it made and touches nothing in infra/terraform or infra/databricks-access.
terraform {
  backend "gcs" {
    bucket = "transit-realtime-tfstate-26"
    prefix = "transit-realtime-pipeline/showcase/dev"
  }
}
