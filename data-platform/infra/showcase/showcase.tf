# The page's "live from the lakehouse" panel reads one public object, v1/showcase.json,
# which a scheduled Databricks job overwrites from Bronze every 10 minutes.
# Rollback: `terraform destroy` here. The bucket and its object go with it.

resource "google_storage_bucket" "showcase" {
  name     = local.bucket
  location = "NORTHAMERICA-NORTHEAST1"

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "inherited"

  # Disposable by design: destroy removes the object too.
  force_destroy = true

  # One object, overwritten every run: keep no old copies.
  versioning {
    enabled = false
  }
  soft_delete_policy {
    retention_duration_seconds = 0
  }

  cors {
    origin          = ["https://ai.linzi.info", "http://127.0.0.1:4321", "http://localhost:4321"]
    method          = ["GET", "HEAD"]
    response_header = ["Content-Type"]
    max_age_seconds = 300
  }

  labels = {
    environment = "dev"
    purpose     = "showcase"
  }
}

# Anyone may read a named object; nobody may list the bucket.
resource "google_storage_bucket_iam_member" "public_object_reader" {
  bucket = google_storage_bucket.showcase.name
  role   = "roles/storage.legacyObjectReader"
  member = "allUsers"
}

# A Databricks identity that can write only this bucket.
resource "databricks_storage_credential" "showcase_writer" {
  name           = "transit_showcase_writer_dev"
  metastore_id   = local.metastore_id
  read_only      = false
  isolation_mode = "ISOLATION_MODE_ISOLATED"
  comment        = "Writes the public showcase JSON (branch showcase)."

  databricks_gcp_service_account {}
}

# Overwriting an object needs delete as well as create.
resource "google_storage_bucket_iam_member" "writer_object_user" {
  bucket = google_storage_bucket.showcase.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${databricks_storage_credential.showcase_writer.databricks_gcp_service_account[0].email}"
}

resource "google_storage_bucket_iam_member" "writer_bucket_reader" {
  bucket = google_storage_bucket.showcase.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${databricks_storage_credential.showcase_writer.databricks_gcp_service_account[0].email}"
}

resource "databricks_external_location" "showcase" {
  name            = "transit_showcase_dev"
  metastore_id    = local.metastore_id
  url             = "gs://${google_storage_bucket.showcase.name}/"
  credential_name = databricks_storage_credential.showcase_writer.name
  read_only       = false
  isolation_mode  = "ISOLATION_MODE_ISOLATED"
  # The IAM grants above can take a minute to apply; skip the immediate check.
  skip_validation = true
  comment         = "Public showcase JSON (branch showcase); destroy with infra/showcase."

  depends_on = [
    google_storage_bucket_iam_member.writer_object_user,
    google_storage_bucket_iam_member.writer_bucket_reader,
  ]
}

output "showcase_url" {
  description = "Public URL the page reads."
  value       = "https://storage.googleapis.com/${google_storage_bucket.showcase.name}/v1/showcase.json"
}

output "showcase_output_uri" {
  description = "Where the Databricks job writes; the bundle variable showcase_output."
  value       = "gs://${google_storage_bucket.showcase.name}/v1/showcase.json"
}
