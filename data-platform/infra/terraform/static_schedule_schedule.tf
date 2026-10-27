# The Scheduler identity can invoke only the static collector Job. It does not
# have landing-bucket or Realtime API-secret access.
resource "google_service_account" "schedule_collector_scheduler" {
  project      = google_cloud_run_v2_job.gtfs_schedule_collector.project
  account_id   = "gtfs-schedule-scheduler"
  display_name = "GTFS Schedule Scheduler"
  description  = "Invokes the static schedule collector job; no feed data or secret access."
}

resource "google_cloud_run_v2_job_iam_member" "schedule_scheduler_invoker" {
  project  = google_cloud_run_v2_job.gtfs_schedule_collector.project
  location = google_cloud_run_v2_job.gtfs_schedule_collector.location
  name     = google_cloud_run_v2_job.gtfs_schedule_collector.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.schedule_collector_scheduler.email}"
}

resource "google_cloud_scheduler_job" "schedule_daily" {
  project     = google_cloud_run_v2_job.gtfs_schedule_collector.project
  region      = google_cloud_run_v2_job.gtfs_schedule_collector.location
  name        = "gtfs-schedule-collector-daily"
  description = "Capture the published static GTFS Schedule once daily at 09:00 America/Vancouver."
  schedule    = "0 9 * * *"
  time_zone   = "America/Vancouver"
  paused      = false

  # This caps the Scheduler-to-Run API request; the Job has its own 300s task timeout.
  attempt_deadline = "60s"

  # Avoid automatic repeated downloads on launch failure; monitor and rerun deliberately.
  retry_config {
    retry_count        = 0
    max_retry_duration = "0s"
  }

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${google_cloud_run_v2_job.gtfs_schedule_collector.project}/locations/${google_cloud_run_v2_job.gtfs_schedule_collector.location}/jobs/${google_cloud_run_v2_job.gtfs_schedule_collector.name}:run"
    body        = base64encode("{}")
    headers = {
      "Content-Type" = "application/json"
    }

    oauth_token {
      service_account_email = google_service_account.schedule_collector_scheduler.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [
    google_project_service.cloud_scheduler,
    google_cloud_run_v2_job_iam_member.schedule_scheduler_invoker,
  ]
}

output "schedule_collector_schedule_name" {
  description = "Independent daily Scheduler job for static GTFS Schedule capture."
  value       = google_cloud_scheduler_job.schedule_daily.name
}

output "schedule_collector_schedule_region" {
  description = "Region of the static schedule Scheduler job."
  value       = google_cloud_scheduler_job.schedule_daily.region
}

output "schedule_collector_scheduler_identity" {
  description = "Dedicated Scheduler identity; separate from the capture runtime identity."
  value       = google_service_account.schedule_collector_scheduler.email
}
