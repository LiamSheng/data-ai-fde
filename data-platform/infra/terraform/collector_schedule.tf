# 调度身份与采集身份分开：前者只能启动现有 Job，不读取 feed key 或写 landing。
resource "google_project_service" "cloud_scheduler" {
  project            = google_cloud_run_v2_job.gtfs_rt_collector.project
  service            = "cloudscheduler.googleapis.com"
  disable_on_destroy = false
}

resource "google_service_account" "collector_scheduler" {
  project      = google_cloud_run_v2_job.gtfs_rt_collector.project
  account_id   = "gtfs-rt-scheduler"
  display_name = "GTFS Realtime Scheduler"
  description  = "Invokes the existing collector job; no feed-secret or landing access."
}

# 添加成员，不接管整个 Job IAM policy，也不在项目上授予 Run Invoker。
resource "google_cloud_run_v2_job_iam_member" "scheduler_invoker" {
  project  = google_cloud_run_v2_job.gtfs_rt_collector.project
  location = google_cloud_run_v2_job.gtfs_rt_collector.location
  name     = google_cloud_run_v2_job.gtfs_rt_collector.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.collector_scheduler.email}"
}

resource "google_cloud_scheduler_job" "collector" {
  project     = google_cloud_run_v2_job.gtfs_rt_collector.project
  region      = google_cloud_run_v2_job.gtfs_rt_collector.location
  name        = "gtfs-rt-collector-every-10-minutes"
  description = "Start one three-feed capture cycle every 10 minutes; no automatic stop."
  schedule    = "*/10 * * * *"
  time_zone   = "Etc/UTC"
  paused      = false

  # 这是启动 API 的等待上限，不是采集任务的运行上限。
  attempt_deadline = "60s"

  # 初期禁用失败重试；这不提供 exactly-once 或跨 execution 的防重叠保证。
  retry_config {
    retry_count        = 0
    max_retry_duration = "0s"
  }

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${google_cloud_run_v2_job.gtfs_rt_collector.project}/locations/${google_cloud_run_v2_job.gtfs_rt_collector.location}/jobs/${google_cloud_run_v2_job.gtfs_rt_collector.name}:run"
    body        = base64encode("{}")
    headers = {
      "Content-Type" = "application/json"
    }

    # Google API 接受 OAuth token，不使用面向 Cloud Run service 的 OIDC token。
    oauth_token {
      service_account_email = google_service_account.collector_scheduler.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [
    google_project_service.cloud_scheduler,
    google_cloud_run_v2_job_iam_member.scheduler_invoker,
  ]
}
