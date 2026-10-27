locals {
  # 来自首次成功构建的结果。后续发布必须审核新的 digest，不使用 latest。
  collector_image_digest = "sha256:c2cdf1da61ad52e79735e93618abf18683b075d87350132b24052e31cb4b5f0e"
  collector_image        = "${google_artifact_registry_repository.collector_images.location}-docker.pkg.dev/${google_artifact_registry_repository.collector_images.project}/${google_artifact_registry_repository.collector_images.repository_id}/collector@${local.collector_image_digest}"
}

# Job 保存运行配置；不设置 execution token，因此创建/更新不会启动一次采集。
resource "google_cloud_run_v2_job" "gtfs_rt_collector" {
  name                = "gtfs-rt-collector"
  project             = google_service_account.gtfs_rt_collector.project
  location            = google_artifact_registry_repository.collector_images.location
  deletion_protection = true

  labels = {
    environment = "dev"
    purpose     = "gtfs-rt-collection"
    # 对首次创建后未就绪的 Job 做一次受控原地更新；不是健康状态声明。
    reconcile_attempt = "1"
  }

  template {
    # 一个 task 内由现有 poller 顺序采集三个 feed，不拆成三个 task。
    task_count  = 1
    parallelism = 1

    template {
      service_account       = google_service_account.gtfs_rt_collector.email
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"

      # 首次验收不自动重跑；5 分钟是 task 的上限，不是轮询周期。
      max_retries = 0
      timeout     = "300s"

      containers {
        image = local.collector_image

        # 沿用镜像 CMD：python -m transit_realtime_pipeline.poller。
        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }

        env {
          name  = "GTFS_RT_STORAGE_BACKEND"
          value = "gcs"
        }

        env {
          name  = "GTFS_RT_GCS_BUCKET"
          value = google_storage_bucket.gtfs_rt_landing.name
        }

        # 三个公开、无凭据的地址属于来源配置；工程资源名称仍供应方无关。
        env {
          name  = "GTFS_RT_VEHICLE_POSITIONS_URL"
          value = "https://gtfsapi.translink.ca/v3/gtfsposition"
        }

        env {
          name  = "GTFS_RT_TRIP_UPDATES_URL"
          value = "https://gtfsapi.translink.ca/v3/gtfsrealtime"
        }

        env {
          name  = "GTFS_RT_SERVICE_ALERTS_URL"
          value = "https://gtfsapi.translink.ca/v3/gtfsalerts"
        }

        # 只保存 secret 引用和版本号。实际值由 Cloud Run 在启动时注入。
        # 使用用户已确认 Enabled 的 Version 1，不读取 secret 值进入 state。
        env {
          name = "GTFS_RT_API_KEY"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.gtfs_rt_api_key.secret_id
              version = "1"
            }
          }
        }
      }
    }
  }

  # 资源引用提供身份/桶/镜像仓库依赖；IAM 授权仍须显式先完成。
  # 不新增 collector 权限，也不把 builder 作为运行身份。
  depends_on = [
    google_project_service.cloud_run,
    google_project_service.cloud_logging,
    google_storage_bucket_iam_member.collector_object_creator,
    google_secret_manager_secret_iam_member.collector_secret_accessor,
  ]
}
