# Changes needed in `peskas.coasts` before Timor can delegate

Companion to [PLAN.md](PLAN.md). Written 2026-08-02 while scoping the removal of
Timor's `rfish-table` dependency. Every item is measured, not inferred — the
evidence is in [STATE.md](STATE.md) Addenda 1–3.

Repo: `WorldFishCenter/peskas.coasts` (local checkout
`~/Desktop/work/wf_projects/peskas.coasts`, currently at `v4.5.0` / `c2bdc58`).
Timor is pinned to `v4.5.0` for the migration, so each of these lands in a new
release that Timor then re-pins to.

---

## Blocking — Timor's weight path cannot move off `rfish-table` without these

### C1. `enrich_taxa()` hardcodes FAO Area 57

`R/fishbase.R`:

```r
dplyr::filter(.data$AreaCode %in% c(NA_integer_, 57))   # Western Indian Ocean
```

**Timor-Leste is FAO Area 71** (Western Central Pacific). Called as-is for Timor
it filters out the species it is meant to enrich. Kenya, Mozambique, Zanzibar are
all Area 51/57, which is why nobody has hit this.

Make the area(s) a config value, e.g. `conf$metadata$fishbase$fao_areas`,
defaulting to the current behaviour so existing countries are unaffected.

### C2. coasts has no length-weight coefficients

`enrich_taxa()` currently emits traits and nutrients only — `Vulnerability`,
`DemersPelag`, `FoodTroph`, `feeding_guild`, `Calcium`, `Iron`, `Omega3`,
`Protein`, `VitaminA`, `Zinc`. **No `a`, no `b`.**

The length-weight fetch exists only in
`peskas.mozambique.data.pipeline/R/model-taxa.R`
(`getLWCoeffs()` / `get_length_weight_batch()`). Upstream it.

**Do not upstream Moz's `Type == "TL"` filter.** Moz does:

```r
dplyr::filter((database == "fishbase" & Type == "TL") | database == "sealifebase", ...)
```

Measured against Timor's known-good coefficient table: no taxon loses *all*
coefficients under TL-only, but four lose more than half their species —
`CJX` 10→3, `EMP` 25→12, `MOB` 9→4, `YDX` 11→4. Since Timor's
`estimate_weight()` aggregates across the species within a taxon code, that
shifts the estimate. Return **all** `Type` values plus `EsQ` and let the caller
filter.

### C3. coasts has no length-length conversion

`grep -rn "length_length"` over `coasts/R/` and Moz's `R/` returns **nothing**.

Timor needs `rfishbase::length_length()` → `aL`, `bL`, `Length1`, `Length2`
(filtered to `Length1`/`Length2` in `TL`/`FL`), because **Timor's v1 survey
records fork length**:

```r
length_type = dplyr::case_when(
  !is.na(length_type)                  ~ length_type,
  survey_version == "v1"               ~ "FL",
  survey_version %in% c("v2", "v3")    ~ "TL")
```

The coefficient table is 2,684 TL rows vs 1,829 FL and 617 SL. Without
length-length, every v1 catch row with a measured length loses its weight.
Moz never hit this because its form records one length type.

Add it alongside C2, keyed the same way (`alpha3_code` → species → coefficients).

### C4. Decide where the enriched snapshot lives

`enrich_taxa()` writes to `conf$storage$google$options` (the **country** bucket),
while `ingestion-pds.R` reads the assets snapshot from
`resolve_storage_opts(conf, "coasts")` (the **hub**). PLAN's Phase 3 flags this
already. The hub holds 118 prod / 22 dev `taxa-fishbase-enriched__*` objects, so
the hub is the de-facto home — make the writer agree with the readers.

---

## Non-blocking, but worth batching into the same release

### C5. No retry logic anywhere in coasts

Confirmed by grep over `pds-api.R` and `ingestion-pds.R` for
`retry` / `insistently` / `req_retry`. Timor keeps
`insistent_upload_cloud_file()` / `insistent_download_cloud_file()`
(`purrr::insistently` + `rate_backoff`) precisely because coasts has no
equivalent — PLAN §10 already lists these as upstream candidates.

Priority case: the PDS **trip** fetch is a single unprotected `httr::GET` for the
whole history and it is what failed run 30637659244.

### C6. `resolve_storage_opts()` has no `public` type

It knows `"coasts"`, `"country"`, `"pds"`. Timor also has a `public_storage`
bucket (the live portal JSON), read directly in `get_public_files()` and
`get_tracks_map()`. Minor.

### C7. Register Timor

- Add `timor` to the `api.trips` block of `peskas.coasts/inst/conf.yml` (needed
  for migration Phase 6).
- Add Timor's PDS customer name to the coasts customer list (Phase 7). **The
  string is not recorded anywhere in Timor's repo** — read it off the PDS API
  first.

---

## Direction reversal to note

PLAN §10 lists **nutrients / RDI** as a Timor→coasts upstream candidate,
"Timor's is the only implementation". That is no longer true: `enrich_taxa()`
already emits `Calcium`, `Iron`, `Omega3`, `Protein`, `VitaminA`, `Zinc`. So the
Phase 8 task is to **compare Timor's `calculate-nutrients.R` against coasts'
output and delete Timor's if they agree** — not to donate it.

Likewise, PLAN sequences Phase 4b (adopt coasts helpers) *before* Phase 10
(upstream to coasts). For weights the dependency runs the other way: **C1–C3
must land before Phase 4b can do anything.**

---

## What Timor gains, beyond consistency

Not just deduplication — measured coverage:

`rfishbase::species()` (Timor's current expansion) is **FishBase-only**. The
known-good pinned table therefore has **zero `a`/`b` for 11 of 56 taxon codes**:
`COZ` cockles, `CRA` crabs, `CUX` sea cucumbers, `IAX` cuttlefish, `OCZ`
octopus, `PEZ` shrimps, `SLV` lobster, `SWX` seaweed — plus `FLY`, `LGE`, `MZZ`.
Eight are invertebrates or algae, i.e. **SeaLifeBase** territory.

Moz's fetch queries `server = "fishbase"` *and* `server = "sealifebase"`, and
`coasts::get_combined_tbl()` / `get_taxa_backbone()` already combine both. So
adopting it should *add* weight coverage for taxa Timor currently cannot weigh
at all — an improvement, not just a refactor.

It also removes an undeclared **GBIF** dependency: `get_catch_types()` calls
`taxize::tax_rank(db = "gbif")` to derive the rank that `get_fish_length()`
branches on. `coasts::expand_taxonomic_info()` uses the FishBase/SeaLifeBase
backbone instead.

---

## Acceptance gate for any replacement

The regeneration has been silently broken for ~21 months (STATE Addendum 1):
every `rfish-table` version since 2026-01-19 has 1,323 rows / 88 species against
the pinned 5,926 / 693. **Do not accept a replacement because "the job ran".**

Required before the pin is removed:

| check | baseline |
|---|---|
| distinct species | **693** |
| coefficient rows | **5,926** |
| taxon codes with ≥1 `a`/`b` | **45 of 56** (target: >45, via SeaLifeBase) |
| mean `a` | 0.02716 |
| mean `b` | 2.9781 |
| length-length rows present | 40 of 56 codes |

Then re-run `calculate_weights()` and diff total catch weight against the Phase 0
golden snapshot. The portal publishes these numbers.

---

## Added after 4.6.0

### C11. `ingest_assets()` writes where the readers no longer look

4.6.0 fixed `enrich_taxa()` to read *and* write through
`resolve_storage_opts(conf, "coasts")` (C4). `ingest_assets()`
([R/ingestion.R](../../peskas.coasts/R/ingestion.R)) was not touched and still
ends with:

```r
upload_cloud_file(
  file = asset_filename,
  provider = conf$storage$google$key,
  options  = conf$storage$google$options    # country bucket
)
```

So the writer targets the country bucket while both readers — `ingestion-pds.R`
and now `enrich_taxa()` — look in the hub. Inside coasts the two resolve to the
same bucket, so it is invisible there; from a downstream package that defines
`storage.google.options_coasts` it means the snapshot is written somewhere
nothing reads. Timor hits this in migration Phase 3.

Fix: resolve through `resolve_storage_opts(conf, "coasts")`, matching C4.

**Status after Timor's migration Phase 3: still open, worked around.** Timor's
`ingest_assets()` calls the coasts function and then re-uploads the snapshot it
left on disk to the hub, so the object exists in both buckets. Two uploads of a
583 Kb file per run. Delete the second one once this lands.

### C13. The assets snapshot has no `country` column

`ingest_assets()` selects `form_id, survey_label, alpha3_code, scientific_name,
english_name` from `taxa`, and the equivalent short lists from `gears`,
`vessels` and `landing_sites`. The frame carries `country` on all four tables
and it is dropped.

Downstream that leaves no way to answer "which of these rows are mine". The
snapshot is cross-country — measured 2026-08-09: **1,609 taxa rows, 96 gears,
49 vessels, 736 sites** across four countries. Timor's 56 `alpha3_code`s are
*all* also used by Kenya, Mozambique or Zanzibar, two of them (`MZZ`, `PWT`)
against a different `scientific_name`, so filtering by code alone silently
mixes another country's taxonomy into a coefficient fetch.

Timor works around it with `metadata.airtable.form_ids` — the record ids of its
two forms — which works because `form_id` is the one selected column that
differs per country. It is a brittle key: hardcoded Airtable record ids in a
config file.

Fix: add `country` to every `select_cols` in `ingest_assets()`. It is additive
and no existing reader would notice.

Related: `landing_sites` also loses `Latitude` / `Longitude`, which are
populated for all 40 Timor sites. Timor does not need them yet (it has
`centro_pescas` in Sheets) but Phase 4's site harmonization would.

### C14. `taxa.length_type` — withdrawn, do not add it

Filed as "add a `length_type` field to the frame `taxa` table", then
**withdrawn the same day.** Timor's Sheets `catch_types` carries a per-taxon
`length_type` for five invertebrates (`SLV` CL, `OCZ` ML, `IAX` ML, `CRA` CW,
`COZ` ShL), but the user confirmed 2026-08-10 that enumerators measure those on
**total length** in the field. The metadata column describes an intent that
field practice does not follow, and Timor's weight code has silently overridden
it to `TL` for years.

So there is nothing to harmonize: propagating the column upstream would
propagate a wrong assertion to every country. If a country ever does record a
non-TL axis, the right shape is not a per-taxon label but a **per-taxon
coefficient filter** on `Type` in the length-weight table — see
`summarise_lw_coeffs()` in Timor's `R/calculate-weights.R`, which currently
pools all axes.

### C12. Trip-fetch window literals (was C9, still open)

`ingest_pds_trips()`'s `dateFrom` and `predict_pds_tracks()`'s `date_from` are
`"2018-01-01"` literals. Correct for Timor — 371 of its 442 devices were last
seen before 2023 — and harmless for the WIO fleets, which have no trips before
2023. Should be config keys, in the same pattern as
`metadata.fishbase.fao_areas`.

### C15. `get_validation_status()` / `update_validation_status()` are not in coasts

Both live in `peskas.mozambique.data.pipeline/R/validation-functions.R` only, so
Timor had to port them in migration Phase 5. Every country that writes flags to
the shared validation database needs the same pair — reading KoBoToolbox's
current status is the only way a pipeline avoids overwriting an approval an
enumerator entered by hand.

Upstream them, with three corrections Timor made in the process:

1. **Add a bulk read.** Moz issues one request per submission. The data endpoint
   returns `_validation_status` alongside `_id` for up to 1,000 submissions per
   request, so `list_validation_statuses(asset_id)` costs `ceiling(n / 1000)`
   requests instead of `n`. Measured on Timor's v2 form: **65 requests and ~70
   seconds against more than twenty minutes** for 7,776 submissions across ten
   `furrr` workers. It also covers every submission rather than only those a
   previous run flagged, so an approval on a never-flagged submission is seen.
2. **Do not let `httr2` throw on 4xx.** KoBoToolbox answers **404** for a
   submission that has never been validated — the normal case. `req_perform()`
   throws by default, so Moz's `resp_status(response) != 200 → "not_validated"`
   branch is unreachable and every unvalidated submission is recorded as
   `fetch_error = TRUE`. Setting `req_error(is_error = function(resp) FALSE)`
   makes the branch work and keeps `fetch_error` meaning a real transport
   failure.
3. **Accept basic auth as well as a token.** Timor's `KOBO_TOKEN` belongs to a
   user with no data access to its assets (200 on `/assets/<id>/`, 404 on
   `/assets/<id>/data/`), while the `KOBO_USERNAME` / `KOBO_PASSWORD` pair
   ingestion already uses works on both. A country should not need a second
   credential for this.

---

## Added in Timor's migration Phase 6 (2026-08-11)

### C16. `resolve_storage_opts()` has no `"api"` type

It knows `"coasts"`, `"country"`, `"pds"` and `"public"`. The cross-country API
bucket is `storage.google.options_api`, so `export_api_raw()` /
`export_api_validated()` have to reach into the config by hand — the one thing
CLAUDE.md tells every Timor call site not to do. Mozambique does the same.
One more `switch()` arm, exactly like C6.

### C17. `summarize_data()` reads two hub artefacts from the country bucket

Measured while verifying Phase 6. `summarize_data()` takes both

```r
asfis        <- download_parquet_from_cloud("asfis", options = conf$storage$google$options)
grid_summaries <- download_parquet_from_cloud(paste0(conf$pds$pds_tracks$file_prefix,
                                                     "-grid_summaries"),
                                              options = conf$storage$google$options)
```

from `storage.google.options`. Inside coasts that **is** the hub
(`peskas-coasts-dev`), where both live — 204 `pds-tracks-grid_summaries__*`
objects, and the `asfis` table. Called with `package = "<country>"` it resolves
the country bucket instead, where neither exists: `timor-dev` has 0 of each.
Same class of bug as C4 and C11, and it is what stops
`summarize_data(package = "peskas.timor.data.pipeline")` running end to end.

Fix: read both through `resolve_storage_opts(conf, "coasts")`. The third read,
the API parquet, already resolves `options_api` correctly and works for Timor
today.

Related, not a bug: `conf$surveys$summaries$file_prefix` has no default and is
absent from coasts' own `inst/conf.yml`; every downstream package must declare
it (Mozambique has `mozambique-summaries`). Timor adds it when it adopts the
function — Phase 7 at the earliest, since `grid_summaries` is a PDS product.
