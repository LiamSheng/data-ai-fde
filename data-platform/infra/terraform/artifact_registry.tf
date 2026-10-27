# 启用容器镜像仓库 API。
# 删除 Terraform 资源时，不自动关闭整个项目的 API。
resource "google_project_service" "artifact_registry" {
  service            = "artifactregistry.googleapis.com"
  disable_on_destroy = false
}

# 保存采集程序的容器镜像，不保存公交原始数据或 API key。
resource "google_artifact_registry_repository" "collector_images" {
  location      = "northamerica-northeast1"
  repository_id = "transit-realtime"
  description   = "Container images for the transit realtime collector."
  format        = "DOCKER"

  labels = {
    environment = "dev"
    purpose     = "gtfs-rt-collection"
  }

  depends_on = [google_project_service.artifact_registry]
}
