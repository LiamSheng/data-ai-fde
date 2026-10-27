# data-platform

Captures TransLink GTFS-Realtime and GTFS Schedule feeds, preserves every raw
snapshot, and models them in Databricks (Bronze → dbt Silver/Gold) into the route
delay data that Genie and the agent service answer from.

This README covers how to run, check and deploy. Design decisions, glossary and
acceptance evidence are kept outside Git in the repository's local `docs/data-platform/`.

## Layout

| Path | What |
| --- | --- |
| `src/transit_realtime_pipeline/` | One-cycle poller (Vehicle Positions, Trip Updates, Service Alerts) and static Schedule capture; local or GCS persistence |
| `tests/`, `scripts/check_ci.py` | Collector checks (offline: lock, Ruff, pytest, skip gate) |
| `Dockerfile`, `cloudbuild.yaml` | Check and runtime images; Cloud Build validates, checks with networking disabled, publishes |
| `infra/terraform/` | GCP: landing bucket, collector and Scheduler identities, Cloud Run Jobs, schedules, build infrastructure |
| `infra/databricks-access/` | Read-only Unity Catalog credential and external location for the landing bucket |
| `databricks/` | Asset bundle: Bronze ingestion notebook, dbt project, `route_delay_summary` function, Genie space |
| `scripts/check_databricks.py` | Databricks-side local checks (ingestion tests, dbt on DuckDB, bundle shape) |

## Setup

Python 3.12 and [uv](https://docs.astral.sh/uv/). Two environments: the root one
(collector, Bronze converters, DuckDB checks) and the dbt one, which pins
protobuf < 7 while the converters need protobuf ≥ 7.

```sh
cd data-platform
uv sync --locked
uv sync --project databricks/dbt --python .venv/bin/python --frozen
```

Enable the pre-push hook once per clone, from the repository root:

```sh
git config core.hooksPath data-platform/.githooks
```

## Checks (offline, no credentials)

```sh
uv run --locked python scripts/check_ci.py           # collector
uv run --locked python scripts/check_databricks.py   # ingestion, dbt, bundle
```

Five collector tests and the local `replay` need private captured snapshots under
`data/` (not in Git). Without them the tests are skipped by an explicit allowlist,
so a green run does not prove they ran.

## Run one poll cycle locally

Set `GTFS_RT_API_KEY`, `GTFS_RT_VEHICLE_POSITIONS_URL`, `GTFS_RT_TRIP_UPDATES_URL`
and `GTFS_RT_SERVICE_ALERTS_URL`, then:

```sh
uv run python -m transit_realtime_pipeline.poller
```

Payloads go to `data/raw/gtfs_rt/<payload_ref>` (content-addressed) and one audit
JSON per feed attempt to `data/ops/capture_attempts/`. `GTFS_RT_STORAGE_BACKEND=gcs`
with `GTFS_RT_GCS_BUCKET` writes to GCS with create-only preconditions. URLs must
not contain credentials or query strings; the key is never logged. On macOS,
`zsh scripts/poll_local.zsh` reads the key from the login Keychain.

TransLink's published terms allow 1,000 requests per API key per day; three feeds
every 10 minutes use 432.

## Deploy

**Collector (GCP).** Build with Cloud Build using the dedicated builder identity,
supplying `_BUILD_SERVICE_ACCOUNT`, `_SOURCE_COMMIT` and
`--gcs-source-staging-dir` (Terraform outputs `build_service_account` and
`build_source_staging_uri`). Pin the accepted image by digest in
`infra/terraform/cloud_run_job.tf`, then:

```sh
cd infra/terraform
terraform fmt -check && terraform validate && terraform plan
```

State lives in a GCS backend. The deployed Jobs and Schedulers (realtime every
10 minutes UTC, static daily 09:00 America/Vancouver) are enabled; an unchanged
configuration should plan no changes. A successful build or Scheduler call is not
a completed capture: check executions, logs and durable audit records together.

**Databricks.** From `databricks/`, with a CLI profile named `transit-dev`:

```sh
databricks bundle validate --target dev --profile transit-dev
databricks bundle deploy   --target dev --profile transit-dev
databricks bundle run transit_slice --target dev --profile transit-dev
```

`transit_slice` is a manual Job: bounded Bronze ingestion, then `dbt build`,
`dbt run-operation deploy_route_delay_summary` and `dbt docs generate`. Consume
Gold only after the whole run and its tests pass. The Genie space can only be
created once its tables exist, so in a fresh workspace deploy, run the Job, then
deploy again. To change the space, edit `databricks/genie/instructions.md` or
`benchmarks.yml`, regenerate with `.venv/bin/python databricks/genie/build_space.py`
(reads the dbt manifest in `databricks/dbt/target/`; run `dbt parse` first in a
fresh clone), and redeploy.

Runs cost Serverless Jobs and SQL warehouse compute. The dev target shares the
`transit_dev_*` schemas, so keep one writer.
