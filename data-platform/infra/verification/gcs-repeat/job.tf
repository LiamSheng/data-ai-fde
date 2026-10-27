variable "verification_id" {
  type        = string
  description = "Fresh lowercase 32-character UUID hex for this isolated check."
  validation {
    condition     = can(regex("^[0-9a-f]{32}$", var.verification_id))
    error_message = "verification_id must be 32 lowercase hexadecimal characters."
  }
}

locals {
  # Application image stays unchanged; separately fingerprint the harness code.
  verification_image  = "northamerica-northeast1-docker.pkg.dev/project-6fed0e0e-2004-4d15-bff/transit-realtime/collector@sha256:c2cdf1da61ad52e79735e93618abf18683b075d87350132b24052e31cb4b5f0e"
  verification_source = file("${path.module}/../../../scripts/verify_gcs_repeat.py")
  verification_sha256 = sha256(local.verification_source)
  verification_bootstrap = join("; ", [
    "import base64, hashlib",
    "source = base64.b64decode('${base64encode(local.verification_source)}')",
    "print('verification_script_sha256=' + hashlib.sha256(source).hexdigest())",
    "exec(compile(source, 'verify_gcs_repeat.py', 'exec'))",
  ])
}

# This directory is a separate Terraform root, not loaded by infra/terraform.
# No IAM, API enablement, bucket, secret, Scheduler or execution is created here.
resource "google_cloud_run_v2_job" "storage_repeat_check" {
  name                = "gtfs-rt-storage-repeat-check"
  project             = "project-6fed0e0e-2004-4d15-bff"
  location            = "northamerica-northeast1"
  deletion_protection = true

  labels = {
    environment = "dev"
    purpose     = "storage-acceptance"
    check_id    = var.verification_id
  }

  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account       = "gtfs-rt-collector@project-6fed0e0e-2004-4d15-bff.iam.gserviceaccount.com"
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"
      max_retries           = 0
      timeout               = "300s"
      containers {
        image   = local.verification_image
        command = ["/app/.venv/bin/python"]
        args = [
          "-c", local.verification_bootstrap,
          "--bucket", "transit-realtime-landing-26",
          "--check-id", var.verification_id,
        ]
        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }
      }
    }
  }
}

output "verification_job_name" {
  value = google_cloud_run_v2_job.storage_repeat_check.name
}

output "verification_prefix" {
  value = "validation/gcs_repeat/${var.verification_id}/"
}

output "verification_script_sha256" {
  value = local.verification_sha256
}
