# 后续构建命令引用这些非敏感输出，不硬编码另一个身份或默认源码桶。
output "build_service_account" {
  description = "Full resource name for the dedicated Cloud Build identity."
  value       = google_service_account.cloud_build_builder.name
}

output "build_source_staging_uri" {
  description = "Pass explicitly as gcloud builds submit --gcs-source-staging-dir."
  value       = "gs://${google_storage_bucket.build_source.name}/source"
}

# 后续人工执行使用 name/region；输出存在不等于已经执行或通过运行验收。
output "collector_job_name" {
  description = "Cloud Run Job name for a separately approved manual execution."
  value       = google_cloud_run_v2_job.gtfs_rt_collector.name
}

output "collector_job_region" {
  description = "Region to use when inspecting or executing the collector job."
  value       = google_cloud_run_v2_job.gtfs_rt_collector.location
}

output "collector_image" {
  description = "Digest-pinned image configured for the collector job."
  value       = local.collector_image
}

output "collector_schedule_name" {
  description = "Cloud Scheduler job controlling recurring collection."
  value       = google_cloud_scheduler_job.collector.name
}

output "collector_schedule_region" {
  description = "Region to use for Scheduler status, pause and resume commands."
  value       = google_cloud_scheduler_job.collector.region
}

output "collector_scheduler_identity" {
  description = "Dedicated invoker; distinct from the collector runtime identity."
  value       = google_service_account.collector_scheduler.email
}
