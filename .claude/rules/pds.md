---
paths:
  - "R/pds-tracks.R"
  - "R/validate-pds-trips.R"
  - "R/merge-trips.R"
  - "R/model-fishery.R"
---

# PDS and modelling

## Timor has no PDS ingestion code

The workflow calls `coasts::ingest_pds_trips()`,
`coasts::ingest_pds_tracks()` and `coasts::preprocess_pds_tracks()` with
`package = "peskas.timor.data.pipeline"`, exactly as Mozambique, Kenya and
Zanzibar do — none of them carries a line of PDS code either. Everything coasts
needs is in `conf$pds`.

`coasts::preprocess_pds_tracks()` (in `preprocess-pds-data`) writes the spatial
grid summaries that `coasts::summarize_data()` reads for the **coasts portal**
(`grid_summaries`). `peskas.timor.portal.v2` does not use them.

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
**not** `coasts::merge_survey_trips()`, which does a different job (it combines
already-matched survey records across regions for the coasts models). Mozambique's
`merge_trips()` is near-identical: the same join key and full join, with its
own I/O; Zanzibar and Kenya carry the same algorithm, so it is an upstreaming
candidate for coasts. It is a **full join**, so its rows are the landings plus the
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

## `coasts::generate_fleet_analysis()` — coasts portal only

It runs in `summarize-model-data`, after `coasts::summarize_data()`, reading
`conf$surveys$summaries` (`timor-summaries`) and the `conf$pds$customers`
device roster, and feeds the coasts portal through `coasts::export_portal()`.
It is **not** used for `peskas.timor.portal.v2`, whose fleet totals come from
`estimate_fishery_indicators()` (glmmTMB). Do not swap one for the other:

- It keys `boat_registry` on `gaul_2_name`. Timor's twelve `geo.total_boats`
  values sit one per reporting unit, but on `gaul_2` rows named `Atabae`,
  `Bazartete`, `Cristo Rei`, `Barique/Natarbora`… — sub-districts, not Timor's
  published `gaul_1`-plus-Atauro vocabulary, which `frame_reporting_region()`
  bridges. Using its output on portal.v2 would rename all twelve regions.
- The portal.v2 contract is the seven JSONs, not `<country>_fishery_metrics`.
