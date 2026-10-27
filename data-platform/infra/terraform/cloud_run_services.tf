# 仅管理 API 启用状态；不创建定时触发器，也不在 apply 时执行采集。
resource "google_project_service" "cloud_run" {
  service            = "run.googleapis.com"
  disable_on_destroy = false
}
