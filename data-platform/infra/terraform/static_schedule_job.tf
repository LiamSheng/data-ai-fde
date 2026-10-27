# Static GTFS Schedule capture is isolated from the Realtime job and its secret.
locals {
  schedule_collector_image_digest = "sha256:f166d05951fda7e2f6df6780ef4186024648f1d74f485adf59d34009487487e2"
  schedule_collector_image        = "${google_artifact_registry_repository.collector_images.location}-docker.pkg.dev/${google_artifact_registry_repository.collector_images.project}/${google_artifact_registry_repository.collector_images.repository_id}/collector@${local.schedule_collector_image_digest}"
}

# The static task needs landing-bucket writes only; it does not read the
# Realtime API key or share the Realtime collector identity.
resource "google_service_account" "gtfs_schedule_collector" {
  account_id   = "gtfs-schedule-collector"
  display_name = "GTFS Schedule Collector"
  description  = "Runtime identity for capturing the public static transit schedule."
}

resource "google_storage_bucket_iam_member" "schedule_collector_object_creator" {
  bucket = google_storage_bucket.gtfs_rt_landing.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.gtfs_schedule_collector.email}"
}

resource "google_cloud_run_v2_job" "gtfs_schedule_collector" {
  name                = "gtfs-schedule-collector"
  project             = google_artifact_registry_repository.collector_images.project
  location            = google_artifact_registry_repository.collector_images.location
  deletion_protection = true

  labels = {
    environment = "dev"
    purpose     = "gtfs-schedule-collection"
  }

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account       = google_service_account.gtfs_schedule_collector.email
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"
      max_retries           = 0
      timeout               = "300s"

      containers {
        image   = local.schedule_collector_image
        command = ["/app/.venv/bin/python"]
        args    = ["-m", "transit_realtime_pipeline.static_schedule"]

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }

        env {
          name  = "GTFS_RT_STORAGE_BACKEND"
          value = "gcs"
        }

        env {
          name  = "GTFS_RT_GCS_BUCKET"
          value = google_storage_bucket.gtfs_rt_landing.name
        }

        env {
          name  = "GTFS_SCHEDULE_URL"
          value = "https://gtfs-static.translink.ca/gtfs/google_transit.zip"
        }
      }
    }
  }

  depends_on = [
    google_project_service.cloud_run,
    google_storage_bucket_iam_member.schedule_collector_object_creator,
  ]
}

output "schedule_collector_job_name" {
  description = "Independent Cloud Run Job for static GTFS Schedule capture."
  value       = google_cloud_run_v2_job.gtfs_schedule_collector.name
}

output "schedule_collector_job_region" {
  description = "Region to use when inspecting or executing the static collector."
  value       = google_cloud_run_v2_job.gtfs_schedule_collector.location
}

output "schedule_collector_image" {
  description = "Digest-pinned image used only by the static schedule collector."
  value       = local.schedule_collector_image
}

output "schedule_collector_runtime_identity" {
  description = "Dedicated runtime service account for the static collector."
  value       = google_service_account.gtfs_schedule_collector.email
}
