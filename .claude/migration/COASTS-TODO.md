# Changes needed in `peskas.coasts` before Timor can delegate

Companion to [PLAN.md](PLAN.md). Written 2026-08-02 while scoping the removal of
Timor's `rfish-table` dependency. Every item is measured, not inferred — the
evidence is in [STATE.md](STATE.md) Addenda 1–3.

Repo: `WorldFishCenter/peskas.coasts` (local checkout
`~/Desktop/work/wf_projects/peskas.coasts`, currently at `v4.5.0` / `c2bdc58`).
Timor is pinned to `v4.5.0` for the migration, so each of these lands in a new
release that Timor then re-pins to.

---

## Status after migration Phase 11a (2026-08-18)

**#17 is merged and shipped.** Merged 2026-08-13 as `989049c` with `--merge`, so
all five commits survive as units of revert, and released as **v4.7.0**; coasts
has since tagged **v4.8.0**, which Timor's container build resolves. All five
commits are ancestors of v4.8.0.

**Timor now actually calls two of them.** C15 and C11 were shipped in 4.7.0 but a
package's own definitions win over its imports, so until Phase 11a deleted the
local copies, Timor's own code was still what executed:

| item | Timor's state after Phase 11a |
|---|---|
| **C15** KoBo validation status | **delegated.** All five local functions deleted; `R/validation.R` calls `coasts::list_validation_statuses()` / `coasts::update_validation_status()`. Verified live: v2 64,997 rows / 74 s, v3 22,285 / 16 s |
| **C11** `ingest_assets()` writes the hub | **workaround deleted.** `coasts::ingest_assets()` resolves the hub itself, so Timor's mirror upload is gone |
| **C13** `country` on the snapshot | **not adopted, and must not be** — see C24 |
| **C16/C17** `resolve_storage_opts(conf, "api")` | in use since Phase 6 |
| **C18/C19** track-id extraction | Timor-side workaround still in place |
| **C21** the secrets leak | fixed upstream; the `log_threshold = logger::INFO` at each call site is kept as a regression guard, and is still absent from all three WIO repos |

Five items, **one commit each**. The commits are the unit of revert, so #17 was
merged with a merge, **never squashed**.

Five items, **one commit each**. The commits are the unit of revert, so #17
must be merged with a merge or rebase, **never squashed**.

| commit | item |
|---|---|
| `c6ed781` | C15 — KoBo validation status |
| `6ef429e` | C13 + C11 — `country` on the snapshot, write to the hub |
| `f4696b8` | C16 + C17 — `resolve_storage_opts(conf, "api")` |
| `9923962` | C18 + C19 — track-id extraction |
| `bf8ac7e` | nutrients — selenium only |

These began as five separate PRs (#12–#16), one per item as PLAN §Phase 10
asked. Consolidated on the user's call 2026-08-13: nothing ships until 4.7.0 is
tagged, so the five could never have merged on independent timelines anyway,
and one branch is one review and one NEWS edit. The five closed PRs carry the
identical commits and their own descriptions, so the per-item reasoning is
still readable there.

Still open and **not** filed as a PR: C12, C20, and the three candidates
Phase 10 decided against donating. Reasons in the STATE entry.

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

**Answered, and they do not agree.** Measured in Phase 4 and acted on in
Phase 8: coasts emits **six** nutrients against Timor's seven, has **no
selenium**, does **no unit conversion** (Timor normalises mg / μg / g per 100 g
to grams), and has **no FAO food-composition override** for the six
invertebrates FishBase cannot estimate (`OCZ`, `IAX`, `COZ`, `PEZ`, `CRA`,
`SLV`) or the hardcoded `FLY` row. The file was renamed to `R/nutrients.R` and
kept, with those four reasons in its header. It goes back to being a Phase 10
**upstream** candidate — coasts' version is the subset.

**Phase 10: split. One of the four gaps is upstreamed, PR
[#17](https://github.com/WorldFishCenter/peskas.coasts/pull/17) `bf8ac7e`; the rest stay
in Timor, on purpose.**

- **Selenium — shipped.** `rfishbase::estimate()` models seven nutrients and
  `enrich_taxa()` selected six. One word. Verified by regenerating the enriched
  table against the live production snapshot: 5,318 rows before and after, 21 →
  22 columns, nothing lost, all 21 pre-existing columns `all.equal()` TRUE, and
  2,540 of 5,318 rows carrying a selenium value. Both servers have the column
  with data (FishBase 5,696 non-NA, SeaLifeBase 359).
- **Unit conversion — not shipped.** The seven nutrients arrive in three
  different units per 100 g. Normalising them upstream silently rescales
  numbers Kenya, Mozambique and Zanzibar already publish. That is a decision
  for those countries, not a default to assume on their behalf.
- **The FAO food-composition override — not shipped.** Substituting values for
  taxa the models cannot estimate is an argument to have, not a default. It is
  also the piece with the most Timor-specific data in it.

Both refusals are now recorded in `?enrich_taxa` alongside the units the table
is actually in, so the next country reads it there rather than re-deriving it.
Timor keeps `R/nutrients.R` whole.

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

**Status after Timor's migration Phase 11a: ✅ fixed upstream and the workaround
is gone.** Shipped in 4.7.0 (commit `6ef429e`); `coasts::ingest_assets()` now
resolves the hub itself and says so in a comment. Timor's mirror upload was
deleted in Phase 11a, verified against v4.8.0's source rather than assumed.
Historical record of the workaround follows.

Timor's
`ingest_assets()` called the coasts function and then re-uploaded the snapshot it
left on disk to the hub, so the object existed in both buckets. Two uploads of a
583 Kb file per run. Delete the second one once this lands.

**Phase 10: fixed, PR [#17](https://github.com/WorldFishCenter/peskas.coasts/pull/17) `6ef429e`
(with C13).** The upload now resolves `resolve_storage_opts(conf, "coasts")`.
Verified by running `ingest_assets(package = "peskas.timor.data.pipeline")` off
the branch — the object landed in `peskas-coasts-dev`. Timor drops its second
upload in Phase 11, after the release exists. No other country calls
`ingest_assets()`: `mozambique-dev` holds zero `assets__*` objects, coasts
itself writes the snapshot.

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

**Phase 10: fixed, PR [#17](https://github.com/WorldFishCenter/peskas.coasts/pull/17) `6ef429e`,
with two deviations from what was filed.**

`country` is added to `taxa`, `gear` and `vessels` — plain text in the frame,
directly usable — and **not** to `sites`. `landing_sites.Country` is a
`multipleRecordLinks` field, so it arrives as an Airtable record id
(`rec8G5G9FZCFZBFyc`), not a name: useless as a filter key. Sites stay keyed by
`form_id`. `latitude` / `longitude` are picked up instead, populated for 343 of
736 sites.

Two things measured while doing it, both of which the filing did not know:

- **Adding the column changes no row counts.** taxa 1,609 → 1,609, gear 96 → 96,
  vessels 49 → 49, sites 736 → 736 — so `dplyr::distinct()` does not fan out and
  the change is purely additive.
- **`taxa.country` carries a trailing newline on the Timor rows** —
  `"Timor-Leste\n"`, in taxa only, not in gears or vessels. It is trimmed in
  `ingest_assets()` before deduplication. Without that, the whole point of the
  column fails silently: `country == "Timor-Leste"` matches nothing.

Selecting Timor by the trimmed column returns **60 taxa, 9 gears, 2 vessels** —
identical to what `metadata.airtable.form_ids` returns today. Timor swaps
`timor_assets()` onto `country` in Phase 11, after the release.

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

**Phase 10: done, PR [#17](https://github.com/WorldFishCenter/peskas.coasts/pull/17) `c6ed781`**
— `R/validation-kobo.R` in coasts, all three functions plus `kobo_request()`,
with all three corrections and a `url` argument defaulting to
`"eu.kobotoolbox.org"` to match `get_kobo_data()`. Verified live against
Timor's v3 asset: 22,250 rows in 22.3 s over 23 requests, `all.equal()` TRUE
against Timor's implementation, and the never-validated single-submission path
returns `not_validated` with `fetch_error = FALSE`. Additive — nothing existing
changed — and it incidentally resolves the dangling `[get_validation_status()]`
link `summarize_data()`'s `@seealso` has carried since before the function
existed anywhere in coasts.

**Phase 11a: ✅ closed.** Shipped in v4.7.0 and Timor's five local copies are
deleted, so `coasts::` is now what executes. Re-verified live against both forms
before the deletion: v2 **64,997** rows in 74.2 s (63,293 not_validated, 1,623
approved, 11 not approved, 70 on hold), v3 **22,285** in 15.5 s — the v3 figure
replicating Phase 10's 22,250 five days later. **The lesson worth keeping: a
green run against a hub release does not prove the hub version is wired in.** A
package's own definitions win over its imports, so the deletion is the test.

---

## Added in Timor's migration Phase 6 (2026-08-11)

### C16. `resolve_storage_opts()` has no `"api"` type

It knows `"coasts"`, `"country"`, `"pds"` and `"public"`. The cross-country API
bucket is `storage.google.options_api`, so `export_api_raw()` /
`export_api_validated()` have to reach into the config by hand — the one thing
CLAUDE.md tells every Timor call site not to do. Mozambique does the same.
One more `switch()` arm, exactly like C6.

**Phase 10: fixed, PR [#17](https://github.com/WorldFishCenter/peskas.coasts/pull/17) `f4696b8`.**
Note that PLAN and the Phase 10 prompt both recorded C16 as *already delivered
in 4.6.0*. It was not — 4.6.0 shipped `"public"` only, and `summarize_data()`
was still reaching into `conf$storage$google$options_api` by hand. Verified
after the change by resolving all five types against coasts', Mozambique's and
Timor's config in both environments; no bucket moves for anyone.

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

**Status after Phase 8: still open, and it is now the *only* thing gating the
grid summaries.** Phase 8 was asked to decide whether to wire
`coasts::preprocess_pds_tracks()` in and run its first pass out of band, and
declined — see C20. C17 is the reason: with `summarize_data()` unable to run
for Timor, the grid summaries have **no reader in this package at all**, and
producing ~1.4 M rows and ~0.8 GB per run for nothing is not a migration step.
Fix C17 and the decision reverses.

### **C17 as filed is wrong. Withdrawn in Phase 10, and the diagnosis was measured**

Neither read is a hub read. Listed 2026-08-13 with the ingestion service
account:

| bucket | `asfis` | `*-grid_summaries__*` |
|---|---|---|
| `mozambique-dev` / `-prod` | 1 / 1 | 18 / 330 |
| `kenya-dev` | 1 | 21 |
| `zanzibar-dev` | 1 | 72 |
| `peskas-coasts-dev` / `peskas-coasts` | **0 / 0** | 205 / 357 |
| `timor-dev` | 0 | 0 |

- **`asfis` is a per-country object.** It exists in every country bucket and in
  neither hub bucket. Moving that read to the hub — the fix C17 asked for —
  would have broken Kenya, Mozambique and Zanzibar on their next run.
- **The grid summaries are written to the country bucket**, by
  `preprocess_pds_tracks()` (`R/preprocessing.R`, `country_opts`). Reader and
  writer already agree. The hub's 205/357 copies exist because for coasts
  *itself* the country bucket **is** the hub.

`timor-dev` holds neither only because Timor has never run either step. So the
real prerequisite for `summarize_data(package = "peskas.timor.data.pipeline")`
is not a coasts fix at all — it is **seeding `asfis` into `timor-dev`,
declaring `surveys.summaries.file_prefix`, and running
`preprocess_pds_tracks()`**. What was genuinely missing was only the `"api"`
arm, which is C16 and is fixed in the same PR.

**This does not by itself reverse the Phase 8 decision.** The grid summaries
still have no Timor consumer — the portal is the `public-timor` JSON contract —
so wiring `preprocess_pds_tracks()` in still buys ~1.4 M rows per run for no
reader. What changes is the reason: it is a Timor decision now, not an upstream
blocker.

---

## Added in Timor's migration Phase 7 (2026-08-11)

### C18. `ingest_pds_tracks()` reads "no tracks stored" instead of failing

`extract_trip_ids_from_filenames()` is

```r
gsub(".*_([0-9]+)\\.parquet$", "\\1", filenames)
```

so a name that does not already match `<something>_<digits>.parquet` is returned
**unchanged** — a full object name where a trip id was expected. The
`setdiff(trips_data, existing_trip_ids)` above it then reports every trip as
new. Measured against `pds-timor-dev` before Timor's conversion: 0 of 99,219
stored tracks recognised, and the first run would have re-fetched **98,472**
tracks from the PDS API. The failure is silent, expensive and looks exactly like
a fresh bucket.

Two cheap fixes, worth both:

1. Derive the id from the configured prefix
   (`sub(paste0("^", prefix, "_"), "", basename(x))`) rather than a generic
   regex, so an unexpected layout yields the whole name and cannot be mistaken
   for an id.
2. `if (length(existing_tracks) > 0 && all(existing_trip_ids == existing_tracks))
   stop(...)` — every listed object failing to yield an id is never a legitimate
   state.

Timor hit this in Phase 7 and worked around it by converting its object family
in place (`data-raw/convert-pds-tracks.R`), not by patching coasts.

**Phase 10: fixed, PR [#17](https://github.com/WorldFishCenter/peskas.coasts/pull/17) `9923962`**
— both suggested fixes, plus three things the filing did not anticipate.
The helper has **three** call sites, not one: `ingest_pds_tracks()` and
`preprocess_pds_tracks()` both pass `conf$pds$pds_tracks$file_prefix` now, and
`backup_tracks()` was calling it on `unique(latest_df$Trip)`, i.e. on trip ids
rather than filenames, where the regex was a no-op — that call is dropped
instead. The prefix now comes in as an argument rather than being assumed. And the change
was checked against every PDS bucket before being made — `pds-mozambique-dev`,
`pds-mozambique-prod`, `pds-kenya-dev`, `pds-zanzibar-dev`, `pds-timor-dev`,
`pds-peskas-coasts-dev` all store `pds-tracks_<id>.parquet` and all yield
identical ids under old and new code. The PR also adds the package's **first
`tests/`**: three assertions over the parser, with the old
`pds-track-<id>__*__.csv.gz` family as the negative case.

### C19. Track objects are the only unversioned Peskas artefact

`ingest_pds_tracks()` writes `sprintf("%s_%s.parquet", prefix, trip_id)` with no
`add_version()`, so a re-ingest of a trip overwrites in place and there is no
history. Defensible — a finished GPS trip is immutable — but it is the one
exception to the `<prefix>__<timestamp>_<sha>__.<ext>` convention every other
object follows, and it means `cloud_object_name(version = "latest")` cannot be
used on the tracks bucket at all. Worth one line of roxygen.

**Phase 10: documented, PR [#17](https://github.com/WorldFishCenter/peskas.coasts/pull/17) `9923962`** —
an `@details` block on `ingest_pds_tracks()`. The behaviour is deliberate and
unchanged; what it now says is *why it matters*, which is that existence is
decided entirely by object name and therefore rests on C18's parser.

### C20. `preprocess_pds_tracks()`'s first run reads the whole history at once

`furrr::future_map_dfr()` over every track object in the bucket, bound into one
in-memory frame before the first write. Incremental afterwards, but the first
run for a country with a long history is unbounded: Timor has 102k tracks
against Mozambique's much shorter deployment. A `batch_size` argument like
`ingest_pds_tracks()` already has — process, write, repeat — would make the
first run survivable on a 7 GB CI runner.

**Status after Phase 8: open, but no longer Timor's blocker.** Phase 8
considered running the first pass locally, as Phase 7 did for the track
conversion, and decided not to — **not because of C20 but because of C17**.
The output has no reader in Timor: it exists to feed `summarize_data()`, which
cannot resolve Timor's inputs, and Timor's portal is the `public-timor` JSON
contract PLAN §2.1 keeps as-is. C20 stays filed for the three countries that do
run it; Timor wires the step in when C17 ships **and** a Timor consumer exists.

### Not a bug: `Traders` is claimable per country

coasts' own `pds.customers` deliberately excludes `Traders` and
`FSSP2: Traders` because they carry `Asia/Kuala_Lumpur` devices that would widen
the filter for every country (COASTS-4.6.0, C7). A **country** config can list
them safely — its own API token only returns its own devices — and for Timor
they are worth 12 IMEIs, 1,412 trips and 20,777 tracked hours. Timor lists all
three.

### C21. `read_config()` logs the whole resolved config — **live secrets leak** ✅ fixed 2026-08-12

`R/utils.R:99`:

```r
logger::log_info("Using configutation: {attr(conf, 'config')}")
logger::log_debug("Running with parameters {conf}")
```

Every workflow function in coasts defaults to `log_threshold = logger::DEBUG`,
so any of them called without an explicit threshold prints the **entire**
resolved configuration into the job log: the GCP service-account private key,
the MongoDB connection string with its password, the KoBo password, and the PDS
and Dataverse tokens. GitHub Actions masks only byte-exact matches of a
registered secret, which a re-serialised JSON key is not.

This is the same line Timor removed from its own `read_config()` in migration
Phase 3, for the same reason. It arrived back through the delegation: the first
`coasts::ingest_pds_trips(package = "peskas.timor.data.pipeline")` run of Phase
7 printed the key to a local log.

**Mozambique, Kenya and Zanzibar all call the coasts PDS functions with the
default threshold**, so this is live in three production pipelines today, not a
Timor-only concern.

Fix: log the key *names* only, as Timor does —

```r
logger::log_debug("Configuration keys: {paste(names(conf), collapse = ', ')}")
```

Timor's workaround until then is to pass `log_threshold = logger::INFO` at every
`coasts::` workflow call site in `data-pipeline.yaml`. That suppresses the line,
because `log_threshold()` is set before `read_config()` runs — but it depends on
every caller remembering, which is exactly what makes it a workaround.

**Fixed by the user on 2026-08-12** in `peskas.coasts`,
`peskas.mozambique.data.pipeline` and `peskas.zanzibar.data.pipeline` (Kenya and
Timor had already fixed their own copies). Timor keeps the `logger::INFO`
argument anyway: its container resolves the latest coasts *release* at build
time, so the fix reaches Timor only once it is tagged, and the argument costs
nothing. **Rotation of the exposed credentials is still outstanding** — the fix
stops new leakage, not what is already in the run history of four repos.

**Confirmed in Phase 10:** the fix is `538b5d0`, merged to `origin/main` as PR
#11 on 2026-08-11, and `read_config()` now logs bucket names only. Note that
the local coasts checkout was **three commits behind `origin/main`** at the
start of Phase 10 (`58a5fdf`, which still had the leak), which is why the phase
prompt described the fix as absent from the code. Always `git fetch` coasts
before reading it. Rotation remains outstanding.

---

## Phase 10 decisions on what was *not* upstreamed

PLAN §Phase 10 says "do nothing" is a defensible outcome per item. Three of the
five candidates and two open items got that answer. Reasons, so the next
session does not re-litigate them:

### `validate_pds_trips()` — deferred, with the donatable split identified

PLAN called it the strongest candidate. It is the strongest *by novelty* — no
coasts equivalent exists — but it fails the test that matters, which is whether
a second country would call it. Kenya, Mozambique and Zanzibar have no PDS trip
validation step at all, so upstreaming it now adds a hub function with no
caller.

It also does not upstream whole. The split, measured against the file:

| part | generic? |
|---|---|
| `merge_consecutive_trips()`, `get_distance()` | **yes** — they operate on the frame `coasts::get_trips()` returns for every country (`Boat`, `Started`, `Ended`, `Trip`, distance, duration) |
| `validate_pds_data()` | **no** — its outlier and quality arms need `start_end_distance`, `outliers_proportion` and `timetrace_dispersion`, which come from Timor's `describe_pds_tracks()`, for which coasts has no equivalent |
| `validate_pds_trips()` itself | **no** — Timor's config keys, its `.rds` contract and its rename onto Timor's ontology |

So the day a second country wants trip merging, the first row is a copy, not a
redesign. Until then it stays in Timor.

### The richer validators — Timor-specific by form, not by intent

Landing regularity, mesh, gleaners, fuel, conservation and happiness all
validate **fields Timor's KoBo forms ask and the other countries' do not**. The
alert-code vocabulary is Timor's contract, and the shared validation UI already
has a per-country dictionary problem (Phase 5 entry). Nothing to donate that
another country could switch on.

### Dataverse — Timor-only by nature

Nobody else publishes to Dataverse. This is the acceptable answer the plan
allowed for.

### C12 (trip-window literals) — left open

`"2018-01-01"` is correct for Timor and harmless for the WIO fleets. Making it
a config key is right, but it is a change to a function all four pipelines call
in exchange for nothing anyone is currently blocked on. Not worth spending the
regression budget on in the same release as five other changes.

### C20 (`preprocess_pds_tracks()` first pass) — left open

Unchanged, and still not Timor's blocker — see the C17 correction above. It
stays filed for the three countries that do run the step.

---

## Added by the 2026-08-18 alignment audit, confirmed in Phase 11a

### C22. `merge_trips()` — four repos, one algorithm

Mozambique's `R/merge-trips.R:20` is **line-for-line Timor's**: the same
`(landing_date, imei)` join, the same `unique_trip_per_day` split, the same
`full_join`, the same comments including the "Merging datasets datasets…" typo,
and it writes parquet. Zanzibar has `merge_trips(site =)`; Kenya documents the
identical algorithm at `R/match-trips.R:285`. This is **not**
`coasts::merge_survey_trips()`, which does C10's different job — inferring from
that function's existence that no country implements this one is the mistake
`CLAUDE.md` made twice.

A real upstreaming candidate that should have been on the Phase 10 list.

### C23. A curated length-weight supplement in the hub

Timor pools **559** manual coefficient rows over 11 mostly-invertebrate codes
(`COZ CRA CUX FLY GZP IAX MOO OCZ PEZ SFA SLV`, 98 species) with the FishBase
fetch, and **4.14%** of national catch weight depends on them.

**Updated 2026-09-08:** they are `inst/extdata/morphometric-coefficients.csv`,
shipped with the package and hand-edited. The earlier note that they "live in
the Google Sheets `morphometric_table`, which is why that table cannot be
deleted" is superseded — Timor reads no Google Sheet at all now. So this is no
longer blocking anything; it is an improvement.

The PESKAS | FRAME base is a label-mapping layer and is the wrong home; the
right one is a coasts-side curated table that `get_taxa_morphometrics()` pools
automatically. No other country has anywhere to put such rows either.

Two questions to settle before moving it, unchanged: whether the table is
**global or per-country** (global means one country's curation moves another's
published catch), and what happens to the **axis residual** — 362 of the 559
rows are fitted on CW/CL/ML/ShL and applied to a total length, ~3.5% of national
catch, and only 12 of 98 curated species have any FishBase conversion.

### C24. `geo.country` is a record-id link, like `landing_sites.Country`

C13 added `country` to `taxa`, `gear` and `vessels` and **explicitly skipped**
`landing_sites` because its `Country` field is a `multipleRecordLinks` holding
Airtable record ids. `geo` (the `districts` table) has the same defect and
nobody checked it. Measured against the live snapshot: `country ==
"Timor-Leste"` returns **37 → 0** rows for `geo`, and `sites` has no `country`
column at all, while `taxa`/`gear`/`vessels` are `identical()` under either key.

**This is why Phase 11a did not make the `timor_assets()` swap C13 anticipated.**
It fails silently: `survey_labels()` applies `timor_assets()` to `sites` and
`geo`, so the swap would strip `landing_site`, `gaul_1_*` and `gaul_2_*` from the
preprocessed table — which feeds the cross-country API export — with no error.

The fix is not a `country` column on those two tables but Mozambique's
`get_airtable_form_id()` (`preprocessing-surveys.R:950`), which resolves the
frame record id from the KoBo asset id at run time and needs no `country` column
at all. It is already live in another repo, and it is what C13's "brittle key"
objection was reaching for. Timor adopts it in Phase 12.

---

## Added by Timor's taxa-path session (2026-09-05)

### C25. The FishBase read is unpinned — a new release changed published catch with no code change ✅ fixed in coasts 4.10.0 (2026-09-06)

**This is the most serious item on this list. It is live in all four
pipelines.** Diagnosed properly 2026-09-05 after a first, wrong diagnosis
(recorded below, because the wrong one is instructive).

`get_combined_tbl()` ([`R/fishbase.R:14`](../../../peskas.coasts/R/fishbase.R))
calls `rfishbase::fb_tbl(tbl_name, server = ...)` with **no `version`**, so
every run takes whatever FishBase release is newest at that moment.

**What made that bite:** `rfishbase` **5.0.3** moved the parquet host from
HuggingFace to Source Cooperative, and the two carry different release sets:

| host | rfishbase | latest release |
|---|---|---|
| `huggingface.co/datasets/cboettig/fishbase` | ≤ 5.0.1 | **25.04** |
| `us-west-2.opendata.source.coop/cboettig/fishbase` | ≥ 5.0.3 | **26.06** |

So a container rebuild picking up a current rfishbase silently moved every
pipeline from FishBase 25.04 to 26.06. **26.06 carries a taxonomic revision:**

```
Caesio, Pterocaesio      Caesionidae -> Lutjanidae
Scarus, Chlorurus, ...   Scaridae    -> Labridae
```

`Caesionidae` and `Scaridae` still exist in the `families` table with **zero
species assigned**. Any taxon whose reference name is one of those families
expands to nothing, receives no coefficients, and weighs `NA` — which sums to
zero. For Timor that is `CJX` (2.1 M individuals, 5% of landed weight, **one of
the 13 `models.modelled_taxa`**) and `PWT`. Verified by reading the 26.06
parquet directly: `Caesionidae` 0 species, `Scaridae` 0 species, `Clupeidae` 15
and `Lethrinidae` 43 unchanged.

Timor's artefact history, now fully explained — every "good" run was local on
rfishbase 5.0.1 / 25.04, every "bad" run was the container on 5.0.3 / 26.06:

| run | where | national catch | `CJX` | `PWT` |
|---|---|---|---|---|
| 2026-08-13 | local, 25.04 | 5,197.4 t | 281.3 t | 25.6 t |
| 2026-08-18 | container, 26.06 | 4,993.1 t | **0 t** | **0 t** |
| 2026-09-03 | local, 25.04 | 5,200.8 t | 281.9 t | 25.8 t |
| 2026-09-04 | container, 26.06 | 4,995.8 t | **0 t** | **0 t** |

**The first diagnosis was "the fetch returns partial data non-deterministically".
It was wrong**, and the evidence that looked like flakiness — 41 of 51 codes
differing by up to 54% between neighbouring runs, and a bad state matching none
of releases 23.01/23.05/24.07/25.04 — was simply the 25.04 → 26.06 diff seen
without knowing 26.06 existed. It is fully deterministic. The lesson worth
keeping: **"it varies between runs" and "it varies between environments" look
identical from the artefacts alone.** Check the resolved release before
concluding either.

**Three asks, in order of value:**

1. **Pin the release.** Give `get_combined_tbl()` a `version` argument sourced
   from configuration (`metadata.fishbase.db_version`, defaulting to `"latest"`
   for compatibility), and thread one resolved version from the top of
   `get_taxa_morphometrics()` — it makes **8** independent reads (`species`,
   `families`, `faoareas`, `poplw`, `popll`, `ecology`, `estimate`, `species`
   again), so today a single run can mix snapshots. Validate per server:
   FishBase has 21.06, SeaLifeBase does not. Published fishery statistics should
   move when somebody decides they move.
2. **Warn on an unmatched name** — see C26. `expand_taxonomic_info()`
   inner-joins, so a name that matches nothing is dropped silently, and that is
   the entire failure mode here.
3. **Log the resolved release and the row count per table**, so an artefact can
   be traced to its inputs after the fact.

Timor has a **local guard** — `assert_taxa_coverage()`
([`R/model-taxa.R`](../../R/model-taxa.R)) fails the run when any taxon except
the two documented exemptions (`MZZ`, `SWX`) resolves to no coefficient pair.
**That guard is what caught this**; without it the pipeline stays green and
publishes a hole, which is what happened on 2026-08-18 and 2026-09-04. Timor
also pins `rfishbase` to 5.0.1 in both Dockerfiles as a stopgap — that pins the
*host*, not the release, and stops working the day HuggingFace serves 26.06.
Only ask 1 fixes it properly, and only for everyone.

### C26. `expand_taxonomic_info()` cannot match a rank between genus and family — ✅ silent-drop half fixed in coasts 4.10.0

`get_taxa_backbone()` pivots `sci_name`, `Genus`, `Family`, `Order` and `Class`,
which is already richer than Mozambique's rank switch. But FAO's ASFIS list
names taxa at ranks FishBase's `families` table does not carry at all — it has
`Family`, `Order`, `Class` and nothing between — so these resolve to nothing and
are dropped by the inner join with no warning:

| ASFIS name | rank | codes affected |
|---|---|---|
| `Thunnini` | tribe | `TUN` — **56% of Timor's landed weight** |
| `Clupeoidei` | suborder | `CLU`, `DCX` |
| `Selachimorpha (Pleurotremata)` | superorder | `SKH` |
| `Brachyura` | infraorder | `CRA` |
| `Actinopterygii` | class, filed by FishBase as `Teleostei` | `MZZ` |

Timor handles this with an explicit alias table, `taxa_search_aliases()`
([`R/model-taxa.R`](../../R/model-taxa.R)), which adds extra search names for a
code rather than replacing its ASFIS name — additive, so nothing that already
resolves can regress, and the published taxon codes are untouched. Mozambique
handles the same problem by recoding `catch_taxon` **in the data**
(`TUN` → `TUS`, `SKH` → `CVX`, `CLP` → `ANX`,
`preprocessing-surveys.R:248`), which also renames the published taxon.

The additive-alias shape is the better of the two and belongs in `coasts` so
every country gets it. **The inner join swallowing an unmatched name is the
underlying defect** — `expand_taxonomic_info()` should at minimum warn when an
input row matches nothing.

### C27. A taxon can silently have no nutrient values

Found while re-measuring nutrients for the same session. `get_nutrients_table()`
is Timor-local, but the shape of the bug is not: the taxon-to-species expansion
feeds both the coefficients and the nutrients, so a code the backbone cannot
resolve gets **neither**, and the nutrient half fails even more quietly than the
weight half — `join_weights()` left-joins and the `NA`s sum to zero.

`TUN` was in exactly that state for the life of the Timor pipeline: **51% of
national catch contributed nothing to any published nutrient figure**, because
`Thunnini` resolved to no species and the common-name rescue that patched the
coefficients never touched the expansion. Fixing the alias (C26) fixed the
nutrients as a side effect, which is precisely the problem — nobody was ever
told.

Any `coasts` function that expands taxa for downstream use should report the
codes it could not resolve. Timor now warns from `get_nutrients_table()` and
errors from `assert_taxa_coverage()`.

### C28. `get_length_length_coeffs()` documents the fit backwards (docs only) ✅ fixed in coasts 4.10.0

The roxygen says the conversion is `Length2 = aL + bL * Length1`. FishBase's
POPLL table fits **`Length1 = a + b * Length2`** — the *second* column is the
predictor. Verified empirically against the table itself: the median `bL` is
**0.958** for `Length1 = FL, Length2 = TL` and **0.827** for `SL, TL`, both
correctly less than 1 because FL and SL are shorter than TL. Under the
documented reading they would have to exceed 1.

No code is wrong — coasts only passes the columns through — but anyone building
a conversion from that sentence inverts every ratio, which silently changes
weight by roughly `(1/r)^b / r^b`, a factor of ~1.1 for FL and ~1.8 for SL.

Two lines while it is open:

1. Fix the sentence.
2. `length_types` defaults to `c("TL", "FL")`, which silently drops every `SL`
   pair — for Timor's taxa that is 1,427 of 2,642 usable conversions, and `SL`
   is where the correction is largest. Either default to `NULL` or say plainly
   in the roxygen that the default cannot convert standard length.


---

## Closed by coasts 4.10.0 (2026-09-06)

`resolve_db_version()` lands, shaped like `resolve_fao_areas()`: explicit
argument, then `metadata.fishbase.db_version`, then `"latest"`, validated
against `available_releases()` **per server**. `get_taxa_morphometrics()` and
`enrich_taxa()` resolve it once at the top and thread that one value through
every read, so a run cannot mix snapshots. `expand_taxonomic_info()` now logs
dropped names at WARN. The POPLL direction in `get_length_length_coeffs()` is
corrected.

**What Timor did with it, same day:**

- `metadata.fishbase.db_version: "25.04"` in `inst/config.yml`, and `conf` is
  now passed to both `get_taxa_morphometrics()` call sites — **without `conf`
  coasts falls back to its own `read_config()` and resolves `"latest"`**, which
  is the drift the key exists to stop. That is the one easy mistake here.
- The `rfishbase` 5.0.1 pin and its build assertion are **deleted** from both
  Dockerfiles; `rfishbase` goes back into `install2.r` unpinned. The pin covered
  the *host*; the config key covers the *release*, which is what actually
  matters. Verified first that the two hosts serve identical 25.04 — 25,730
  `poplw` rows and 27,211 `popll` rows on both — so dropping the package pin
  changes nothing.
- Re-ran `get_morphometric_tables()` against 4.10.0: **coefficients identical**
  to the pre-pin run, so no re-baselining.
- **coasts >= 4.10.0 is now a hard floor** for Timor.

C26's remaining half is still open: the *additive alias* shape
(`taxa_search_aliases()`) belongs upstream so every country gets it. Only the
silent-drop warning was fixed.

---

### C29. The assets snapshot cannot resolve a KoBo asset id to a form record id

Filed 2026-09-07 (Timor migration Phase 12).

`coasts::get_assets()` takes `form_ids` — Airtable **record** ids — and its own
roxygen points at `get_airtable_form_id()` to obtain them. That function is not
in `coasts`. **Each of Kenya, Mozambique and Zanzibar carries its own local
copy** (`R/airtable.R`, `R/preprocessing-surveys.R:975`,
`R/airtable-helpers.R:18`), and Timor was asked in ALIGNMENT-AUDIT §15 to adopt
a fourth. Three copies of a lookup is the signal it belongs in the hub.

Worse, the lookup cannot be done from the snapshot at all. `ingest_assets()`
writes a `forms` table with exactly two columns:

```
forms: form_id | form_name
```

where `form_id` is the **KoBo asset id** — not the Airtable record id, which is
what every *other* table's `form_id` column contains. So the same column name
means two different things across the snapshot, and the mapping between them is
absent from the object being filtered. `geo` does carry `airtable_id`; `forms`
does not.

The consequence is that resolving ids at run time requires a **live Airtable
call**, which is what the three WIO copies do — putting an Airtable round trip
on jobs that are otherwise pure GCS reads, and allowing a resolved id that is
not present in the snapshot version being read.

**Two changes, both small:**

1. `ingest_assets()` should keep `airtable_id` on `forms`, as it already does on
   `geo`. Then the mapping travels with the data.
2. `get_assets()` should accept KoBo asset ids and do the lookup itself from
   that column, keeping `form_ids` for callers that already hold record ids.

**What Timor did, 2026-09-07:** adopted the live-API lookup, so it now matches
Kenya, Mozambique and Zanzibar exactly — `get_airtable_form_id()` +
`timor_form_ids()` in [`R/get-cloud-files.R`](../../R/get-cloud-files.R), and
`metadata.airtable.form_ids` is **deleted**. Verified the lookup returns
precisely the two ids that were hardcoded, one per form, with the asset tables
unchanged (60/9/2/40/37). `timor_form_ids()` caches per session because
`timor_assets()` is called ten times a run; measured at one round trip (1.4 s)
with later calls at 0.002 s.

So this is now a **cleanup, not a blocker**: it is a fourth copy of the same
lookup and a live Airtable dependency on three jobs that are otherwise pure GCS
reads. Landing C29 lets all four countries resolve from the snapshot and delete
all four copies.

**Update 2026-09-07: the read-side delegation shipped, and it was never blocked
by this.** Timor's `get_assets()` now calls `coasts::get_assets()` and
`timor_assets()` is deleted. The earlier note below claimed the swap was blocked
because `get_assets()` cannot return `forms` — that only held while the ids were
to be resolved *from the snapshot*. Resolving them from the live API makes the
snapshot's `forms` table irrelevant. C29 remains worth doing to remove the API
call and the four duplicate lookups, but it blocks nothing.

**Verified for Timor:**
`coasts::get_assets()`'s defaults are exactly right here. Its five tables
(`taxa`, `gear`, `vessels`, `sites`, `geo`) cover **every** reader in the
package — the snapshot's `forms`, `devices` and `frame` have none, and
`metadata$devices` in `validate_landings()` is the *Google Sheets* table, not
the frame's. Its `drop_cols = c("country", "latitude", "longitude")` is safe:
`country` rides on `taxa`/`gear`/`vessels`/`geo` and `latitude`/`longitude` on
`sites`, no reader touches any of them, and dropping them before `distinct()`
changes no row count (37 geo / 60 taxa / 9 gear / 2 vessels / 40 sites either
way). The only thing blocking the swap is that `get_assets()` cannot return
`forms`, which Timor would need in the same download to resolve its own ids.
