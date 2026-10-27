# 手动提交构建时上传源码压缩包；不是公交原始数据，也不是 Terraform state。
# 名称包含项目的短标识，以降低全局桶名冲突的可能。
resource "google_storage_bucket" "build_source" {
  name     = "transit-realtime-build-source-6fed0e0e-26"
  location = "NORTHAMERICA-NORTHEAST1"

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = false
  }

  # 暂存源码可由 Git 重新取得；7 天后进入删除流程，不用于永久追溯存档。
  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  # 被生命周期删除的对象仍有约 7 天恢复窗口，期间仍占用存储。
  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  # 这是 Terraform 计划层保护，不是 GCS 对象保留锁。
  lifecycle {
    prevent_destroy = true
  }

  labels = {
    environment = "dev"
    purpose     = "build-source-staging"
  }
}
