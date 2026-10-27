# 名称、存储策略、授权等“容器信息”，由 Terraform 管理


# 启用 Secret Manager API，并由 Terraform 跟踪。
# 将来删除这个 Terraform 资源时，不自动关闭整个项目的 API。
resource "google_project_service" "secret_manager" {
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

# 只创建 secret 的名称、存储位置和元数据。
# 不在 Terraform 中创建包含真实 API key 的 secret version。
resource "google_secret_manager_secret" "gtfs_rt_api_key" {
  secret_id = "gtfs-rt-api-key"

  # 沿用当前 landing bucket 所在区域。
  replication {
    user_managed {
      replicas {
        location = "northamerica-northeast1"
      }
    }
  }

  labels = {
    environment = "dev"
    purpose     = "gtfs-rt-collection"
  }

  # 在创建 secret 前，先完成 API 启用。
  depends_on = [google_project_service.secret_manager]

  # 防止仍由本配置管理时，意外规划删除或替换 secret。
  lifecycle {
    prevent_destroy = true
  }
}

# 仅允许专用采集身份读取这一项 secret 的值。
# 不授予它读取整个项目所有 secrets 的权限。
resource "google_secret_manager_secret_iam_member" "collector_secret_accessor" {
  project   = google_secret_manager_secret.gtfs_rt_api_key.project
  secret_id = google_secret_manager_secret.gtfs_rt_api_key.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.gtfs_rt_collector.email}"
}