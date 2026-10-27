# Cloud Run 采集程序未来使用的运行身份。
# 不创建或下载长期 Service Account key。
resource "google_service_account" "gtfs_rt_collector" {
  account_id   = "gtfs-rt-collector"
  display_name = "GTFS Realtime Collector"
  description  = "Runtime identity for collecting realtime feeds into the landing bucket."
}

# 权限只授予现有 landing bucket，不在整个项目上授权。
# 使用 iam_member 添加这一条授权，避免接管或替换其他成员的授权。
resource "google_storage_bucket_iam_member" "collector_object_creator" {
  bucket = google_storage_bucket.gtfs_rt_landing.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.gtfs_rt_collector.email}"
}
