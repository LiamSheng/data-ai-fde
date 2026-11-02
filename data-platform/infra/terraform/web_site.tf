# 网站 ai.linzi.info（KAN-28）：全球外部 Application Load Balancer 作为统一入口。
#
#   ai.linzi.info ──A 记录──> 全球静态 IP
#     :443 HTTPS 转发规则 → HTTPS 代理（Google 托管证书）→ URL map
#                                                          └ 默认 → backend bucket（web bucket + Cloud CDN）
#     :80  HTTP 转发规则  → HTTP 代理 → 只做跳转的 URL map（301 到 https）
#
# 阶段 4 在 URL map 里加 /api/* → serverless NEG → Cloud Run（agent-service），前后端同域。
# 费用：项目里前 5 条全球转发规则合计 $0.025/小时（约每月 $18），这里用 2 条。

locals {
  web_domain = "ai.linzi.info"
}

# 负载均衡、地址、证书都属于 Compute Engine API。只确保启用，destroy 时不关闭。
resource "google_project_service" "compute" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

# 网站文件：web/dist 由 web/scripts/publish.sh 上传。可公开读取单个对象，但不能列出 bucket。
resource "google_storage_bucket" "web" {
  name     = "ai-linzi-info-web-26"
  location = "NORTHAMERICA-NORTHEAST1"

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "inherited"

  force_destroy = false

  # 负载均衡读取 bucket 时使用这两项：/blog/ 对应 blog/index.html，找不到的路径返回 404.html。
  website {
    main_page_suffix = "index.html"
    not_found_page   = "404.html"
  }

  # 每次发布都会覆盖文件；旧版本没有保留价值。
  versioning {
    enabled = false
  }

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  labels = {
    environment = "dev"
    purpose     = "web-site"
  }
}

# 与 status bucket 相同：legacyObjectReader 只含 storage.objects.get，匿名用户不能列出对象。
resource "google_storage_bucket_iam_member" "web_public_object_reader" {
  bucket = google_storage_bucket.web.name
  role   = "roles/storage.legacyObjectReader"
  member = "allUsers"
}

# 负载均衡的后端：web bucket 加 Cloud CDN。缓存时长由上传时写入的 Cache-Control 决定：
# /_astro/*（文件名带内容哈希）一年并 immutable，其余 no-cache，发版后立即生效。
resource "google_compute_backend_bucket" "web" {
  name        = "ai-linzi-info-web"
  bucket_name = google_storage_bucket.web.name
  enable_cdn  = true

  compression_mode = "AUTOMATIC"

  cdn_policy {
    cache_mode = "USE_ORIGIN_HEADERS"
  }

  depends_on = [google_project_service.compute]
}

# 一个全球 anycast IP，DNS 的 A 记录指向它；443 和 80 两条转发规则共用。
resource "google_compute_global_address" "web" {
  name = "ai-linzi-info-ip"

  depends_on = [google_project_service.compute]
}

# Google 托管证书：DNS 指向上面的 IP 之后才会签发，状态从 PROVISIONING 变为 ACTIVE。
resource "google_compute_managed_ssl_certificate" "web" {
  name = "ai-linzi-info-cert"

  managed {
    domains = [local.web_domain]
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [google_project_service.compute]
}

resource "google_compute_url_map" "web" {
  name            = "ai-linzi-info"
  default_service = google_compute_backend_bucket.web.id
}

resource "google_compute_target_https_proxy" "web" {
  name             = "ai-linzi-info-https"
  url_map          = google_compute_url_map.web.id
  ssl_certificates = [google_compute_managed_ssl_certificate.web.id]
}

resource "google_compute_global_forwarding_rule" "web_https" {
  name                  = "ai-linzi-info-https"
  target                = google_compute_target_https_proxy.web.id
  ip_address            = google_compute_global_address.web.id
  port_range            = "443"
  load_balancing_scheme = "EXTERNAL_MANAGED"
}

# HTTP 只做一件事：301 跳到同一路径的 HTTPS。
resource "google_compute_url_map" "web_http_redirect" {
  name = "ai-linzi-info-http-redirect"

  default_url_redirect {
    https_redirect         = true
    redirect_response_code = "MOVED_PERMANENTLY_DEFAULT"
    strip_query            = false
  }

  depends_on = [google_project_service.compute]
}

resource "google_compute_target_http_proxy" "web_http_redirect" {
  name    = "ai-linzi-info-http"
  url_map = google_compute_url_map.web_http_redirect.id
}

resource "google_compute_global_forwarding_rule" "web_http" {
  name                  = "ai-linzi-info-http"
  target                = google_compute_target_http_proxy.web_http_redirect.id
  ip_address            = google_compute_global_address.web.id
  port_range            = "80"
  load_balancing_scheme = "EXTERNAL_MANAGED"
}

output "web_ip" {
  description = "Point the A record for ai.linzi.info here."
  value       = google_compute_global_address.web.address
}

output "web_bucket" {
  description = "Bucket that web/scripts/publish.sh uploads web/dist to."
  value       = google_storage_bucket.web.name
}

output "web_url" {
  description = "The site, once DNS points at web_ip and the certificate is ACTIVE."
  value       = "https://${local.web_domain}/"
}
