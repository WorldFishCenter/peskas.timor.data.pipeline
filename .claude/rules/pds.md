---
paths:
  - "R/pds-tracks.R"
  - "R/validate-pds-trips.R"
  - "R/merge-trips.R"
  - "R/model-fishery.R"
---

# PDS and modelling

## Timor has no PDS ingestion code

The workflow calls `coasts::ingest_pds_trips()` and
`coasts::ingest_pds_tracks()` with `package = "peskas.timor.data.pipeline"`,
exactly as Mozambique, Kenya and Zanzibar do — none of them carries a line of
PDS code either. Everything coasts needs is in `conf$pds`.

The third call those three make, `coasts::preprocess_pds_tracks()`, is **not**
wired in, deliberately: its output feeds `coasts::summarize_data()`, which
Timor does not use (its portal is the JSON contract) and which is blocked for
Timor anyway on COASTS-TODO C17 — so it would produce ~1.4 M grid rows per run
for no reader. Wire it in when C17 ships **and** a Timor consumer exists.

`describe_pds_tracks()` is the one PDS product coasts has no equivalent for —
per-trip descriptors (`start_end_distance`, `outliers_proportion`,
`timetrace_dispersion`, start/end coordinates) that `validate_pds_trips()`
joins on. `coasts::preprocess_pds_tracks()` emits spatial grid summaries
instead.

## Device selection is a denylist, on purpose

`conf$pds$exclude_customers` is what `coasts::ingest_pds_trips()` filters by.
Timor's token returns only Timor's trips, so the whole job is dropping the two
non-fishing projects (`Traders`, `FSSP2: Traders` — bicycle journeys). An
**allowlist would silently discard the history of any tracker later
reassigned**, because `pds_devices` records who owns a device *now* while the
trips are historical. coasts requires exactly one of `customers` or
`exclude_customers`.

The other three countries share one token returning six countries' trips, so
they cannot use a denylist; they set `select_by: community`, which keeps a trip
if it *happened* at one of the country's communities. Timor does not need it.

## `merge_trips()`

Matches a landing to a tracked trip on `(landing_date, tracker_imei)`. It is
**not** `coasts::merge_survey_trips()`, which does a different job (COASTS-TODO
C10); Mozambique's `merge_trips()` is line-for-line identical to this one
(COASTS-TODO C22). It is a **full join**, so its rows are the landings plus the
validated tracker trips minus the actual matches. Its output `all_trips__*.rds`
feeds `format_public_data()` and `model-fishery.R` — changing its schema breaks
the export path.

## `frame_reporting_region()`

The single definition of Timor's twelve published reporting regions from the
frame's `geo`: Atauro by `gaul_2_name` (a GAUL sub-district of Dili that Timor
reports separately), then `Lautém` / `Liquiçá` / `Oecussi` recoded to the
published spellings. Four explicit cases, **not** an accent strip —
`iconv(x, "UTF-8", "ASCII//TRANSLIT")` yields `Laut'em` / `Liquic'a` on macOS
and matches nothing.

## `coasts::generate_fleet_analysis()` — assessed, not adopted

It is the standard's analogue of `estimate_fishery_indicators()`: it raises
sampled catch to a fleet total from a registered-boat count, the same job and
the same input field (`geo.total_boats`), by a different estimator (PDS trip
rates rather than glmmTMB). Three reasons, in order:

1. **It cannot run.** Its `monthly_summaries` input comes from
   `coasts::summarize_data()`; `conf$surveys$summaries` is `NULL` for Timor and
   that chain is deliberately unwired.
2. **It would rename all twelve published regions.** It keys `boat_registry` on
   `gaul_2_name`. Timor's twelve `geo.total_boats` values sit one per reporting
   unit, but on `gaul_2` rows named `Atabae`, `Bazartete`, `Cristo Rei`,
   `Barique/Natarbora`… — sub-districts, not Timor's published
   `gaul_1`-plus-Atauro vocabulary. `frame_reporting_region()` exists precisely
   to bridge that.
3. The portal contract is the seven JSONs, not `<country>_fishery_metrics`.

Revisit only if C17 ships, a Timor consumer for `summarize_data()` appears,
*and* the registry key is reconciled.
