# 网页读取的 Landing 状态（landing/v1/latest.json，格式见 contracts/landing-status/）。
# 与 Landing bucket 分开：这里的对象会被覆盖、可以公开读取；Landing 只新建、保持私有。
resource "google_storage_bucket" "gtfs_rt_status" {
  name     = "transit-realtime-status-26"
  location = "NORTHAMERICA-NORTHEAST1"

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  # 允许下面的 allUsers 只读授权。2026-10-31 查询：项目的有效组织策略
  # constraints/storage.publicAccessPrevention 未强制开启。
  public_access_prevention = "inherited"

  force_destroy = false

  # latest.json 每轮覆盖一次。覆盖在 GCS 里等于删除旧对象再新建：
  # 不保留旧版本，也不软删除被覆盖的对象，否则每天上百份旧副本会持续计费。
  # （周期记录在 Landing bucket，只新建不覆盖，那里的 7 天软删除照常保留。）
  versioning {
    enabled = false
  }

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  # 浏览器直接从网站读取 latest.json。网站部署前只允许本地开发地址（web/README）。
  cors {
    origin          = ["http://127.0.0.1:4321", "http://localhost:4321"]
    method          = ["GET", "HEAD"]
    response_header = ["Content-Type"]
    max_age_seconds = 300
  }

  labels = {
    environment = "dev"
    purpose     = "gtfs-rt-status"
  }
}

# 匿名用户只能读取单个对象。legacyObjectReader 只含 storage.objects.get；
# objectViewer 还含 storage.objects.list，会让匿名用户列出 bucket 里的所有对象。
resource "google_storage_bucket_iam_member" "status_public_object_reader" {
  bucket = google_storage_bucket.gtfs_rt_status.name
  role   = "roles/storage.legacyObjectReader"
  member = "allUsers"
}

# collector 要读取上一份 latest.json 并覆盖它；覆盖已存在的对象需要删除权限，
# objectCreator 不够。只授予 status bucket，Landing bucket 仍只有 objectCreator。
resource "google_storage_bucket_iam_member" "collector_status_object_user" {
  bucket = google_storage_bucket.gtfs_rt_status.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.gtfs_rt_collector.email}"
}

output "status_bucket" {
  description = "Bucket for GTFS_RT_STATUS_BUCKET; holds only landing/v1/latest.json."
  value       = google_storage_bucket.gtfs_rt_status.name
}

output "latest_status_url" {
  description = "Public URL the web page reads."
  value       = "https://storage.googleapis.com/${google_storage_bucket.gtfs_rt_status.name}/landing/v1/latest.json"
}
