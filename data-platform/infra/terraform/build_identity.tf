# 构建身份与 gtfs-rt-collector 分开；不创建或下载长期私钥。
resource "google_service_account" "cloud_build_builder" {
  account_id   = "transit-rt-builder"
  display_name = "Transit Realtime Image Builder"
  description  = "Checks and builds collector images; no feed-secret or deployment access."

  depends_on = [google_project_service.cloud_build]
}

# 构建程序只读上传的源码。上传由提交构建的人完成，不给 builder 源码写权限。
resource "google_storage_bucket_iam_member" "builder_source_viewer" {
  bucket = google_storage_bucket.build_source.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.cloud_build_builder.email}"
}

# 只授权这一座镜像仓库，不授予整个项目的 Artifact Registry Writer。
resource "google_artifact_registry_repository_iam_member" "builder_image_writer" {
  project    = google_artifact_registry_repository.collector_images.project
  location   = google_artifact_registry_repository.collector_images.location
  repository = google_artifact_registry_repository.collector_images.repository_id
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.cloud_build_builder.email}"
}

# Logs Writer 使用项目级授权；不包含修改日志配置或读取业务 secrets 的权限。
# iam_member 仅增加这个成员，不接管同角色的其他成员或整个 IAM policy。
resource "google_project_iam_member" "builder_log_writer" {
  project = google_service_account.cloud_build_builder.project
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.cloud_build_builder.email}"

  depends_on = [google_project_service.cloud_logging]
}
