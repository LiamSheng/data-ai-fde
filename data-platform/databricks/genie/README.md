# Route delay Genie space

The chat interface of Epic 3 (KAN-8, ticket KAN-18). Terms are in `CONTEXT.md`; the
decision that numbers only come from `route_delay_summary` is ADR 0001.

## What is deployed

`databricks bundle deploy` creates or replaces the space from
`route_delay.geniespace.json` (resource `resources/route_delay.genie.yml`). The space
holds:

- tables `fct_trip_delay_sample`, `dim_routes`, `route_aliases`
- SQL function `route_delay_summary`, the only source of delay figures
- the text in `instructions.md`
- one benchmark per phrasing in `benchmarks.yml` that has `calls`

A deploy replaces the whole definition, so change these files, not the Genie UI.

Benchmarks deploy with the space: the `serialized_space` format (Genie API, version 2)
has a `benchmarks.questions` section, and each question there needs exactly one SQL
answer (checked 2026-10-19 against the Databricks Genie API docs; bundle validated
with CLI v1.16.1).

Table and function names come from the local dbt manifest, which targets the dev
schemas (`transit_dev_*`). The bundle has only the dev target; a target with another
`schema_prefix` would need its own generated file.

## Changing questions or instructions

1. Edit `instructions.md` or `benchmarks.yml`.
2. Regenerate: `.venv/bin/python databricks/genie/build_space.py` (reads the dbt
   manifest in `databricks/dbt/target`, so table and function names follow dbt).
3. `uv run --locked python scripts/check_databricks.py`: the checker fails on a
   stale JSON, and replay runs every benchmark call on local data, failing on a
   route name that does not resolve.

## Running the benchmark (cloud)

Creating the space fails until its tables exist ("Table ... does not exist", 403), so
in a workspace where the Job has not built Epic 3 yet, deploy, run the Job, then deploy
again:

```
databricks bundle deploy -p transit-dev   # updates the Job; the space step fails
databricks bundle run transit_slice -p transit-dev
databricks bundle deploy -p transit-dev   # creates the space
```

Then in the space open **Benchmarks**, run all questions in **Chat** mode, and record the
accuracy and failing questions on KAN-18. Each question runs as a new conversation and
is rated Good when Genie's result set matches the answer SQL.

The questions under `expected` in `benchmarks.yml` have no SQL answer, which a
serialized benchmark requires, so they are not deployed. Ask them in the space's chat
and check the reply against `expected` by hand.

Benchmarks in Chat mode check the numbers, not the wording. If replies drop the
sample counts or say "trips were late", add Agent-mode evaluation notes (KAN-18,
decision 4).
