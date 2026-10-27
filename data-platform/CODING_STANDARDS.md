# Coding standards

Judgement rules for review. Mechanical rules live in the checks instead
(`scripts/check_ci.py`, `scripts/check_databricks.py`); this file holds only what no
check can decide.

## Bronze inputs

**Pinned content stays loadable.** Bronze is insert-only, so an existing schema hides
what a fresh one would lack. When a change stops reading an ingestion input, everything
a dbt var pins (`reference_static_sha256`, the `sample_*` attempt ids and hashes) must
still load into a fresh `transit_*_bronze` schema. Check `dbt_project.yml` vars and the
job parameters against what `load()` reads.

## Row identity

**Key GTFS-Realtime rows by position, not by trip.** An entity in a snapshot is
`(payload_sha256, entity_index)`, a stop update adds `update_index`. `trip_id` repeats
within one snapshot (see *Delay sample* in `CONTEXT.md`), so a key, join or
`unique_key` test on `trip_id` within a snapshot is a defect.
