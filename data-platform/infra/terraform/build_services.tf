# API 启用状态也由 Terraform 跟踪；移除资源时不关闭整个项目的 API。
resource "google_project_service" "cloud_build" {
  service            = "cloudbuild.googleapis.com"
  disable_on_destroy = false
}

# cloudbuild.yaml 使用 CLOUD_LOGGING_ONLY，不另建 GCS 日志桶。
resource "google_project_service" "cloud_logging" {
  service            = "logging.googleapis.com"
  disable_on_destroy = false
}
