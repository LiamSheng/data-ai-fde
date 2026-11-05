# Showcase branch

Temporary. Adds a "Live from the lakehouse" section under the Map page workspace on
ai.linzi.info, refreshed every 10 minutes from Databricks Bronze. It is built on the
unaccepted KAN-35 Auto Loader code and skips acceptance on purpose; it never merges
into `main`.

## What it adds (and nothing else)

| Piece | Where | Isolated how |
|---|---|---|
| Public bucket `transit-realtime-showcase-26` + a Databricks write credential and external location | `data-platform/infra/showcase/` | Own Terraform state; `terraform destroy` removes all of it |
| Job `transit-showcase-dev`, every 10 min: Auto Loader → `transit_showcase_bronze`, then `showcase_export.py` writes `v1/showcase.json` | `data-platform/databricks/resources/showcase.job.yml`, `databricks/showcase/` | Own schema and checkpoints; never touches `transit_dev_*` |
| Web section, hidden until `showcase.json` loads | `web/src/components/ShowcasePanel.astro`, `web/src/lib/showcase-client.ts` | Missing or broken JSON leaves the page as on `main` |

Development mode deploys every schedule paused and refuses to unpause them by default,
so resume the Job's schedule by hand after each `bundle deploy`. `check_local.py` allows
this one scheduled Job.

## Deploy (Mac)

```sh
git fetch origin && git switch showcase

# 1. Bucket and write access (gcloud ADC + the transit-dev Databricks profile)
cd data-platform/infra/showcase
terraform init && terraform apply

# 2. Job (first run reads Landing from showcase_since, default 2026-11-04T00:00:00 UTC)
cd ../../databricks
export DATABRICKS_CONFIG_PROFILE=transit-dev
databricks bundle deploy
databricks bundle run transit_showcase      # one run now; check it passes
# Then Jobs UI → transit-showcase-dev → Schedules & Triggers → Resume.
# Every later `bundle deploy` pauses it again.

# 3. Check the file, then publish the site
curl -s https://storage.googleapis.com/transit-realtime-showcase-26/v1/showcase.json | head -c 400
cd ../../web && ./scripts/publish.sh
```

## Stop or roll back

- **Stop spending now:** pause the schedule of `transit-showcase-dev` in the Jobs UI.
- **Page:** `git switch main && cd web && ./scripts/publish.sh` (the section is gone).
- **Databricks:** from `main`, `cd data-platform/databricks && databricks bundle deploy`
  (the bundle deletes Jobs it no longer defines), then
  `DROP SCHEMA workspace.transit_showcase_bronze CASCADE`.
- **GCP and Databricks access:** on this branch, `cd data-platform/infra/showcase && terraform destroy`.
- **Branch:** `git branch -D showcase && git push origin --delete showcase`.

Each step stands alone; doing only the first two already hides the section and stops cost.

## Known gaps (accepted for a showcase)

- Not accepted on Databricks: serverless `foreachBatch` imports, the `PATH_NOT_FOUND`
  handling, `dbutils.fs.put` to the external location and nested schema equality are
  all first exercised by the first scheduled run. If a run fails, the section shows the
  last good file, or stays hidden.
- 144 serverless runs a day; whether the free-trial credit covers DBUs is unverified.
