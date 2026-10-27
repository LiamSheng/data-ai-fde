# Metadata lookup only. The bucket itself remains owned by infra/terraform.
data "google_storage_bucket" "landing" {
  name = local.landing_bucket

  lifecycle {
    postcondition {
      condition     = self.location == "NORTHAMERICA-NORTHEAST1" && self.uniform_bucket_level_access
      error_message = "Confirm the Landing bucket region and uniform access before creating this connection."
    }
  }
}

# Databricks generates the identity; Terraform receives only its public email.
resource "databricks_storage_credential" "landing_reader" {
  name           = "transit_landing_reader_dev"
  metastore_id   = local.metastore_id
  read_only      = true
  isolation_mode = "ISOLATION_MODE_ISOLATED"
  comment        = "Read-only access to the transit Landing bucket."

  databricks_gcp_service_account {}

  lifecycle {
    prevent_destroy = true
  }
}

# Add individual memberships without taking ownership of the full bucket policy.
resource "google_storage_bucket_iam_member" "landing_object_viewer" {
  bucket = data.google_storage_bucket.landing.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${databricks_storage_credential.landing_reader.databricks_gcp_service_account[0].email}"
}

resource "google_storage_bucket_iam_member" "landing_bucket_reader" {
  bucket = data.google_storage_bucket.landing.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${databricks_storage_credential.landing_reader.databricks_gcp_service_account[0].email}"
}

resource "databricks_external_location" "landing" {
  name               = "transit_landing_dev"
  metastore_id       = local.metastore_id
  url                = "gs://${data.google_storage_bucket.landing.name}/"
  credential_name    = databricks_storage_credential.landing_reader.name
  read_only          = true
  isolation_mode     = "ISOLATION_MODE_ISOLATED"
  fallback           = false
  enable_file_events = false
  skip_validation    = false
  comment            = "Transit source files; ingestion must exclude validation/ objects."

  depends_on = [
    google_storage_bucket_iam_member.landing_object_viewer,
    google_storage_bucket_iam_member.landing_bucket_reader,
  ]

  lifecycle {
    prevent_destroy = true
  }
}
