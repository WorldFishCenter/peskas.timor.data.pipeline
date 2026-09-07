# Export the raw trips table to the cross-country API bucket

Projects the **weighted** (pre-validation) long catch table onto the
22-column schema `peskas-api-{dev,prod}` publishes for every country,
and uploads it to `conf$api$trips$raw$cloud_path`.

## Usage

``` r
export_api_raw(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The (standard Apache logj4) log level used as a threshold for the
  logging infrastructure. See
  [logger::log_levels](https://daroczig.github.io/logger/reference/log_levels.html)
  for more details.

## Value

no outputs. This function is used for its side effects.

## Details

The schema was read off the live `peskas-api-prod` objects (2026-08-10)
and is the same 22 columns, in the same order, for Kenya, Mozambique and
Zanzibar, raw and validated alike:

    survey_id, trip_id, landing_date, gaul_1_code, gaul_1_name, gaul_2_code,
    gaul_2_name, landing_site, n_fishers, trip_duration_hrs, gear, vessel_type,
    catch_habitat, catch_outcome, n_catch, catch_taxon, scientific_name,
    length_cm, catch_kg, catch_price, tot_catch_kg, tot_catch_price

Three Timor-specific decisions, all recorded in the migration Phase 6
entry of `.claude/migration/STATE.md`:

- **Grain.** The API is one row per (trip, catch); Timor's long tables
  are one row per (submission, catch, **length bin**). The bins are
  collapsed: `catch_kg` is their sum and `length_cm` their mean weighted
  by the number of individuals counted in each bin. Empty bins are kept
  in the input and contribute nothing to either, so no catch record is
  lost.

- **`trip_id`** is `TRIP_<submission_id>`, as in all three other
  countries — a trip identifier, not the PDS `tracker_trip_id`.
  [`coasts::summarize_data()`](https://rdrr.io/pkg/coasts/man/summarize_data.html)
  does `group_by(trip_id) |> slice(1)`, so it must be unique per
  landing, and only half of Timor's landings match a tracked trip.

- **`catch_price`** is `NA`: Timor records revenue per landing, not per
  catch. The landing's revenue is `tot_catch_price`, which is what
  Mozambique — also without per-catch prices — publishes.
