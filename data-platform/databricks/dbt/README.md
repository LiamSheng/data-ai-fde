# Transit transformations

Local dbt Core project for the selected Vehicle Positions + Static slice and the
Trip Updates delay slice (Epic 3, KAN-8).
The execution guide is the local `docs/data-platform/ops/dbt-core-acceptance.md` (not in Git).

## Ownership

- Python/Spark owns `workspace.transit_dev_bronze`: capture attempts, immutable RT
  entities and versioned routes and calendars. dbt declares these as sources and never rebuilds them.
- dbt owns eleven Silver views in `workspace.transit_dev_silver` and three Gold Delta
  tables in `workspace.transit_dev_gold`. The standard schema macro adds the
  `_silver` / `_gold` suffixes to target schema `transit_dev`.
- Holiday Day types come from the latest Static calendar (`service_date_overrides`
  view), not a hand-maintained seed. The one seed, `route_aliases`, maps names such
  as "B-Line" to `route_id`s; `dbt build` loads it into `transit_dev_silver`.
- The Unity Catalog function `transit_dev_gold.route_delay_summary` is defined in
  `macros/route_delay_summary.sql` and deployed by the Job's
  `dbt run-operation deploy_route_delay_summary` after `dbt build`. Its cases in
  `dev/function_tests.yml` run offline with `dev/duckdb_check.py function`.
- DAB describes one manual Job and its ordered tasks. Terraform retains ownership
  of Landing access and IAM. No schedules or warehouse resources are defined here.

## Environment

Python 3.12, Core 1.12.5, adapter 1.12.7. The adapter currently excludes Core
1.12.6; do not independently upgrade only one pin. `uv.lock` fixes local transitive
dependencies. Cloud task environments pin the same direct dependencies but may
resolve different transitive/platform packages; inspect runtime versions if debugging.

`profiles.yml` contains no secrets. Local SQL execution uses browser OAuth, not an
assumption that the existing CLI Keychain login automatically authenticates dbt.
Platform dbt tasks use their generated warehouse profile/Run as credentials.
Do not copy platform tokens into this file or override them with local browser auth.

From the repository root:

```sh
uv sync --project databricks/dbt --python .venv/bin/python --frozen
databricks/dbt/.venv/bin/python databricks/check_local.py
```

### Running SQL locally on DuckDB

dbt unit tests and data tests otherwise run only on the warehouse. For fast local
evidence, `dev/duckdb_check.py` compiles offline, transpiles the Databricks SQL to
DuckDB with sqlglot and runs it. It uses the **root** venv, because the Bronze
converter needs protobuf ≥ 7 while dbt pins protobuf < 7:

```sh
.venv/bin/python databricks/dbt/dev/duckdb_check.py unit             # all unit tests
.venv/bin/python databricks/dbt/dev/duckdb_check.py unit --select NAME
.venv/bin/python databricks/dbt/dev/duckdb_check.py replay           # local captures
```

`replay` loads the successful Static and Trip Updates captures under `data/` through
the Bronze converters, builds every model, runs every data test and prints a per-snapshot summary
of `fct_trip_delay_sample` (`--sql` prints any other query). A pass is evidence, not
acceptance: Databricks remains the source of truth, and dialect differences can hide
or invent failures (for example `dayofweek()` numbers days differently in the two
engines, so models use `date_format(..., 'EEE')`). Data tests honour their
`fail_calc` / `warn_if` / `error_if`, so a warning prints `WARN` and does not fail.
It is not deployed with the bundle.

The checker parses dbt, compiles models/data tests with introspection and cache
population disabled, checks the compiled SQL syntax and local conversion behavior, and
checks bundle shape against the installed CLI schema. It does not authenticate,
start compute, validate live privileges, execute SQL or validate DAB deployment state.

## Model graph

```text
Bronze sources → stg_capture_attempts ─┬→ int_vehicle_observations → route_vehicle_counts
                stg_vehicle_positions┘                         └→ snapshot_status
                stg_routes → dim_reference_routes ─────────────→ route_vehicle_counts
                stg_routes → dim_routes ───────────────────────┐
                stg_trip_updates ──────────────────────────────┴→ fct_trip_delay_sample
```

Each attempt remains a separate observation even when it references unchanged
payload bytes. `route_vehicle_counts` only handles FULL_DATASET observations and
counts distinct nonblank vehicle IDs. Missing keys fail model quality checks;
unmatched Static routes remain visible through a left join. `snapshot_status`
also represents empty full snapshots and marks differential snapshots unsupported.

The selected Static content version is an explicit label reference:
`static_linkage_status=unverified_reference`. No historical effective-date rule
has been established. Freshness uses the audit completion timestamp minus feed
timestamp. Neither dbt build success nor table modification time proves a feed SLA.

The selected-sample reconciliation expects 2 attempts, 779 entities, 239 routes
and 779 observed vehicles, based on the supplied sample output. For a different
sample, update the sample variables/expectations together, or explicitly set
`verify_sample: false` and agree replacement reconciliation evidence.

`fct_trip_delay_sample` holds one Delay sample per snapshot (distinct Trip Updates
payload) and non-canceled trip, measured at its Next stop (terms in `CONTEXT.md`,
decisions in ADR 0001, local `docs/data-platform/adr/`). Statistics are computed on
it at query time; there is no hourly aggregate table. Samples carry the Route display
name from `dim_routes`, built from the latest Static version already in Bronze (not the
fixed `reference_static_sha256`). Samples of unmatched routes are kept without a name;
`route_unmatched_sample_share` warns above `route_unmatched_share_warn` (1%) and fails
above `route_unmatched_share_error` (5%).

Model tables can be created before every downstream test finishes; a failed dbt
build is not an atomic rollback. Only consume Gold after the full run and tests
succeed. Genie publication and recurring ingestion are later work.
