resource "google_storage_bucket" "gtfs_rt_landing" {
  name     = "transit-realtime-landing-26"
  location = "NORTHAMERICA-NORTHEAST1"

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  force_destroy = false

  versioning {
    enabled = false
  }

  soft_delete_policy {
    retention_duration_seconds = 604800 # 7 天
  }

  # Terraform 计划层的删除/替换保护；这不是 GCS 保留锁。
  lifecycle {
    prevent_destroy = true
  }

  labels = {
    environment = "dev"
    purpose     = "gtfs-rt-landing"
  }
}
