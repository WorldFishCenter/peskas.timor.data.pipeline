# Alignment audit — static assets and pipeline logic

**Read this beside [`.github/workflows/data-pipeline.yaml`](../../.github/workflows/data-pipeline.yaml).**
It is ordered by pipeline step, in DAG order, not by table and not by severity.
Every claim carries the measurement behind it; measurements were taken
2026-08-18 against the `-dev` buckets with `R_CONFIG_ACTIVE` unset. Nothing was
written anywhere.

This is an assessment, not a change. No file in `R/`, `inst/config.yml` or
`.github/workflows/` was touched.

---

## 0. Summary

### The twelve Google Sheets tables

| # | table | live reader | disposition | blocked on |
|---|---|---|---|---|
| 1 | `vms_installs` | **none** | **Delete** | nothing |
| 2 | `centro_pescas` | **none** | **Delete** | nothing — and the config comment on this key is factually wrong |
| 3 | `boats` | `data_report.Rmd` only | **Delete with the report section**, else keep | the `data-report.yaml` re-enable decision |
| 4 | `fishing_vessel_statistics` | `data_report.Rmd` only | **Delete with the report section**, else keep | same |
| 5 | `registered_boats` | `model-fishery.R:32` (workflow) | **Move to the frame** — `geo.total_boats` | 2 of 12 values disagree; the swap moves national catch **−4.74%** |
| 6 | `devices` | `validation.R:59` | **Keep, measured** | Airtable `pds_devices` is missing **144** deployed IMEIs |
| 7 | `stations` | `validation.R:98` + `enumerators_summary.Rmd` (active) | **Keep, measured** | 11 of 40 names differ; 15 spellings hardcoded across 3 files |
| 8 | `reporting_units` | `validation.R:98` | **Keep, measured** | only **1** of the 18 disagreements is real; 9 are diacritics, 8 are Atauro |
| 9 | `catch_types` | `preprocessing-surveys.R:537`, `pds-maps.R:379`, `test_validated_landings.R:70` | **Keep, shrink to 3 columns** | nothing |
| 10 | `morphometric_table` | `model-taxa.R:36` (workflow) | **Keep — target is the hub, not the frame** | no coasts home for curated coefficients |
| 11 | `habitat` | `preprocessing-surveys.R:559` | **Keep** — propose a frame table | Airtable schema proposal |
| 12 | `conservation` | `validation.R:121` | **Keep** — propose a frame table | Airtable schema proposal |

Two deletable now (`vms_installs`, `centro_pescas`), two deletable with a
report decision (`boats`, `fishing_vessel_statistics`), one movable after an
Airtable reconciliation (`registered_boats`), seven keepers with numbers.

### Pipeline logic

| # | divergence | class | §  |
|---|---|---|---|
| L1 | **`merge_trips()` has three cross-country counterparts, not zero** | **unexamined drift** — the recorded reason is false | [§9](#9-merge_trips) |
| L2 | The raw-column passthrough superset survives to `calculate_weights()` — 59 columns, 1.65 M rows | **deliberate but undocumented**; it has exactly one live reader | [§7](#7-merge_landings--calculate_weights) |
| L3 | Two different "North Coast" definitions inside one published object | **unexamined drift** — a live bug, 14.97% of revenue | [§11](#11-format_public_data--export_files--enumerators-report) |
| L4 | `coasts::generate_fleet_analysis()` — the fourth standard step Timor does not run | **unexamined drift** — never mentioned outside one PLAN table row | [§12](#12-the-standard-steps-timor-does-not-run) |
| L5 | The API export mixes frame `gaul_*` with Sheets `landing_site` | **deliberate but undocumented** | [§8](#8-export_api_raw--export_api_validated) |
| L6 | `timor_assets()`'s Phase 11 swap onto `country` works for 3 of 5 tables | **new constraint on Phase 11** | [§2](#2-ingest_assets) |
| L7 | `get_preprocessed_metadata()` and `get_preprocessed_sheets()` are body-identical | **unexamined drift** — dead duplicate | [§1](#1-ingest_metadata_tables--preprocess_metadata_tables) |
| L8 | `harmonise_v2()`/`harmonise_v3()` vs WIO `map_surveys()` | **deliberate** — they solve different problems | [§4](#4-preprocess_landings) |
| L9 | `coasts::summarize_data()` — prerequisites are three local items | **deliberate**, re-examined; reason still holds | [§12](#12-the-standard-steps-timor-does-not-run) |

---

## 1. `ingest_metadata_tables()` → `preprocess_metadata_tables()`

**Job:** `ingest-preprocess-metadata-tables`, steps 1 and 2.
**Cross-country:** no equivalent exists. Mozambique and Zanzibar declare five
Sheets tables each and read none; Kenya declares one and only *writes* a sheet
in `R/export.R`. Timor is the only pipeline that still ingests metadata.

`ingest_metadata_tables()` pulls the 12 tables into `metadata-tables__*.rds`;
`preprocess_metadata_tables()` runs each through a `pt_validate_*` parser and
writes `metadata-tables_preprocessed__*.rds`. Measured shapes:

```
devices             596 x  4      stations             40 x  7
device_installs     465 x 11      reporting_unit       17 x  6
boats               461 x 22      habitat               7 x  2
catch_types          61 x  9      vessels_stats        24 x  4
morphometric_table  559 x 16      registered_boats     12 x  2
centro_pescas       152 x 10      conservation          5 x  2
```

Both objects are trivially small — 33 versions of each total under 10 MB in
`timor-dev`. **Cost is not the argument for removing any of these; a wrong
lookup is.**

### L7 — a dead duplicate accessor

[`get_preprocessed_metadata()`](../../R/get-cloud-files.R#L293) and
[`get_preprocessed_sheets()`](../../R/get-cloud-files.R#L348) are
**body-identical** — same prefix, same provider, same
`resolve_storage_opts(conf, "country")`. Every live call site uses
`get_preprocessed_sheets()`; the only caller of the other is
`inst/report/unanswered_summary.Rmd`, whose driver `generate_form_summary.R`
lost its workflow when `form-summary.yaml` was deleted in Phase 9.

**Disposition:** delete `get_preprocessed_metadata()` and
`unanswered_summary.Rmd`+`generate_form_summary.R` in Phase 11. It is exported,
so it needs a `NAMESPACE` regeneration.

### Table 1 — `vms_installs` → **Delete**

Parsed by `pt_validate_vms_installs()` into `device_installs`. Grepped for
`$device_installs` across `R/`, `inst/report/`, `inst/tinytest/` and
`data-raw/`: **zero matches.** The one place it would have been used is
[`validation.R:57`](../../R/validation.R#L57), which says so in a comment — "the
`device_installs` narrowing has been switched off for years".

`pt_validate_vms_installs()` also contains a commented-out `stop()` and a
`ok_boat_installs` frame that is computed and discarded.

**Delete the table, the parser and the list element.** No reader, no risk.

### Table 2 — `centro_pescas` → **Delete, and correct the config comment**

Zero readers of `$centro_pescas`. `pt_validate_centro_pescas()` is a
pass-through placeholder.

`inst/config.yml:310` says `centro_pescas  # keep: the only source of
landing-site lat/lon`. **That is wrong.** The table's ten columns are:

```
Name, site_number, Status, aldeias, suco, suco_name, administrative_posts,
administrative_post (from administrative_posts), municipality,
aldeias (from administrative_posts)
```

There is no latitude and no longitude anywhere in it. It is an
administrative-nesting table (152 rows, 35 distinct `site_number`).

Separately, the frame **does** now carry coordinates: all **40 of 40** Timor
sites in `assets$sites` have non-`NA` `latitude` and `longitude` (the Phase 10
C13 PR). So even if a coordinate reader ever appears, the answer is the frame.

**Delete the table, the parser and the list element**, and delete the comment
rather than migrating it.

### Tables 3 & 4 — `boats`, `fishing_vessel_statistics` → **Delete with the report**

`$boats` matches only [`data_report.Rmd:183`](../../inst/report/data_report.Rmd#L183)
(a boat-length / engine-power histogram). `$vessels_stats` matches only
[`data_report.Rmd:48`](../../inst/report/data_report.Rmd#L48), where it is
summed by `reporting_region` into a `vessels_stats` object — **duplicating what
`registered_boats` already provides**, from a table with 24 rows instead of 12.

`data-report.yaml` is `disabled_inactivity` and its re-enable is a Phase 11
action. So these two are not "no reader" — they are "one reader in a workflow
that has not run since 2026-06-01".

**Disposition: DELETE. User decision, 2026-08-18 — drop the boats section.**
So Phase 11 removes, together: the boats histogram in `data_report.Rmd`, the
`vessels_stats` summary at its line 48, both config entries, and both parsers
(`pt_validate_boats()`, `pt_validate_vessels_stats()`). The boats histogram
described a 461-row registry maintained outside this pipeline, and
`vessels_stats` was a worse-shaped duplicate (24 rows summed to 12) of
`registered_boats`, which the pipeline already reads.

---

## 2. `ingest_assets()`

**Job:** `ingest-preprocess-metadata-tables`, step 3.
**Cross-country:** identical — every country calls `coasts::ingest_assets()`.

The snapshot read back from `peskas-coasts-dev` on 2026-08-18 confirms both
Phase 10 columns shipped:

```
geo       92 x 10  (country present, but see below)
taxa    1609 x  6  (country ✔)
gear      96 x  4  (country ✔)
vessels   49 x  4  (country ✔)
sites    736 x  6  (latitude ✔ longitude ✔, no country)
forms     14 x  2
devices  910 x 14
frame    407 x 11
```

### L6 — the Phase 11 `timor_assets()` swap only works for three of five tables

COASTS-TODO C13 records the intent: "Timor swaps `timor_assets()` onto
`country` in Phase 11, after the release." Measured against the live snapshot:

| table | `form_ids` rows | `country == "Timor-Leste"` rows | identical? |
|---|---|---|---|
| `taxa` | 60 | 60 | **TRUE** |
| `gear` | 9 | 9 | **TRUE** |
| `vessels` | 2 | 2 | **TRUE** |
| `geo` | 37 | **0** | — |
| `sites` | 40 | n/a (no column) | — |

`geo` **does** carry a `country` column, and it is a `multipleRecordLinks`
field — the same problem C13 already found on `landing_sites.Country`. Its four
distinct values are Airtable record ids (`rec7DrrSnRrlzv8BF` for Timor). It is
not a country name and never becomes one.

`timor_assets()` is applied to `sites` and `geo` in
[`survey_labels()`](../../R/preprocessing-surveys.R#L503) and to `taxa` in
[`get_taxa_list()`](../../R/model-taxa.R#L208).

**Disposition:** Phase 11 must **not** swap `timor_assets()` onto `country`
wholesale. Three options, in order of preference:

1. **Leave `timor_assets()` on `form_id`.** It works for all five tables today
   and is verified identical to `country` for the three that have it. The
   "brittle key" objection in C13 is about hardcoded record ids, not correctness.
2. **Adopt Mozambique's `get_airtable_form_id()`** (`preprocessing-surveys.R:950`),
   which resolves the frame record id from the KoBo asset id at run time. This
   removes the hardcoded ids without needing a `country` column at all, and it
   is already live in another repo. This is the real fix C13 was reaching for.
3. Swap only taxa/gear/vessels and keep `form_id` for sites/geo — a
   two-mechanism `timor_assets()`, worse than either of the above.

**Recommendation: option 2**, as part of the proposed Phase 12, not Phase 11.

---

## 3. `ingest_landings()`

**Job:** `ingest-landings`. Nothing static-asset here.
**Cross-country:** the same shape — every country ingests raw KoBo to parquet
via `coasts::get_kobo_data()`. Timor's `ingest_landings(versions = c("v2","v3"))`
does in one function what Moz does in `ingest_landings_lurio()` +
`ingest_landings_adnap()` and Zanzibar in `ingest_wcs_surveys()` +
`ingest_wf_surveys()`. Measured raw widths: v2 **311** columns, v3 **275**.

**Class: deliberate and documented.** No finding.

---

## 4. `preprocess_landings()`

**Job:** `preprocess-landings`.

### L8 — `harmonise_v2()`/`harmonise_v3()` vs `map_surveys()`

These are **not** counterparts and comparing them as such is a category error.

- WIO `map_surveys()` (Moz `preprocessing-surveys.R:1071`, Zanzibar
  `preprocessing.R:731`) resolves *labels*: it joins each frame mapping and
  **drops the raw column at every join** —
  `select(-c("catch_taxon","form_id","english_name"))`,
  `-c("gear","form_id")`, `-c("vessel_type","form_id")`,
  `-c("landing_site","form_id")` — then renames the standard name into the
  standard slot.
- Timor's `harmonise_v2()`/`harmonise_v3()` reconcile *form versions*:
  they coalesce differently-spelled KoBo questions between v2 and v3 so the two
  can be `bind_rows()`d into one table. The WIO repos have no such function
  because they never merge two form versions — each form keeps its own
  `preprocess_*()` all the way to `merge_trips()`.

Timor's actual counterpart to `map_surveys()` is
[`resolve_survey_labels()`](../../R/preprocessing-surveys.R#L573), which does
the same four joins in the same order.

**Class: deliberate.** The divergence follows from decision PLAN §2.2 (one
merged table across form versions) and should be documented as such rather than
"fixed". The one thing `resolve_survey_labels()` does *not* do that
`map_surveys()` does is drop the raw columns — see L2 in §7.

### Which Sheets tables this step reads

[`survey_labels()`](../../R/preprocessing-surveys.R#L503) is the only
preprocessing reader, and it takes exactly two things from the Sheets:

- `sheets$catch_types` → `length_type`, joined onto the frame taxa by
  `survey_label` / `catch_number`.
- `sheets$habitat` → the `habitat_code` → `habitat_type` lookup.

Everything else (`taxa`, `gear`, `vessels`, `sites`, `geo`) comes from the
frame. That half of PLAN §2.5 is complete.

### Table 9 — `catch_types` → **Keep, shrink to three columns**

Nine columns. Measured against the frame's 60 Timor taxa rows:

| claim | measurement |
|---|---|
| `interagency_code` vs frame `alpha3_code` | **0 disagreements** across all 60 shared `survey_label`s |
| Sheets rows with no frame counterpart | **1** — `catch_number = 0`, "No catch", no `alpha3_code` |
| `catch_name_en` vs frame `english_name` | **55 of 59 differ** |
| `length_type` non-`NA` | **5** — `SLV` CL, `OCZ` ML, `IAX` ML, `CRA` CW, `COZ` ShL |

The 55/59 is the number that keeps this table. The Sheets `catch_name_en`
carries **display names** ("Short bodied mackerel", "Jacks/Trevally/Other
Scad", "Sardines/pilchards"); the frame `english_name` carries **FAO names**
("Indian mackerels nei", "Carangids nei", "Herrings, sardines nei"). They are
different vocabularies for the same code, and the frame's is not a substitute.

Three live readers, and each takes a different column:

1. [`survey_labels()`](../../R/preprocessing-surveys.R#L537) — `catch_number`,
   `length_type`.
2. [`convert_taxa_names()`](../../R/pds-maps.R#L373), called by
   `format_public_data()` — `interagency_code`, `catch_name_en`.
3. [`test_validated_landings.R:70`](../../inst/tinytest/test_validated_landings.R#L70)
   — `interagency_code`, as the valid-code allowlist. **A live test assertion;
   do not break it.**

Plus `data-raw/freeze-landings-v1.R:78`, which uses `Length2`/`aL`/`bL` from the
*morphometric* table, not this one.

Two things worth recording about reader 2, both measured:

- **The five-name filter in `convert_taxa_names()` is a de-duplicator, not a
  content filter.** `catch_types` has 4 duplicated `interagency_code`s (`SUR`,
  `IHX`, `MZZ`, plus the `NA` on row 0). After
  `filter(!catch_name_en %in% c("Herring","Unknown","Surgeonfish","Bannerfish","No catch"))`
  the count of duplicated codes is **0**. There is no `distinct()` on the join,
  so without that filter every `SUR`/`IHX`/`MZZ` catch row would fan out to two
  and be double-counted in the published `groups_comp`. It works, but it works
  by accident of five hand-picked names.
- **`convert_taxa_names()`'s output column `Common name` is never read on the
  live path.** Its only consumer is `pds-maps.R:468`, inside `ingest_pds_map()`,
  which Phase 8 established has no workflow. `format_public_data()` uses only
  `fish_group`, which is assigned by a hardcoded `case_when()` over
  `catch_taxon` and does not touch the Sheets at all.

- **`length_type` reaches no published object.** It is carried into
  `landing_catch` by `get_weight()` ([format-public-data.R:722](../../R/format-public-data.R#L722))
  and re-nested, but none of the seven `portal-*.json` objects contains it, and
  it is not one of the 22 API columns. Confirmed by reading all seven published
  JSONs and the API parquet. COASTS-TODO C14 stays withdrawn.

**Disposition: keep, and narrow the table to `catch_number`,
`interagency_code`, `catch_name_en`, `length_type`.** The other five columns
(`Name`, `catch_name_tt`, `catch_family`, `isscaap_taxocode`, `isscaap_family`)
have no reader anywhere. Record in the config: *"kept for `catch_name_en` — 55
of 59 display names differ from the frame's FAO `english_name` — and for the
`catch_number = 0` 'no catch' row."*

### Table 11 — `habitat` → **Keep; propose a frame table**

Seven rows, two columns: `Reef 1, FAD 2, Deep 3, Beach 4, Traditional FAD 5,
Mangrove 6, Seagrass 7`. Read by
[`survey_labels()`](../../R/preprocessing-surveys.R#L559); the resolved
`habitat` then reaches `catch_habitat` in the **cross-country API export** and
the `catch_habitat`/`revenue_habitat`/`nutrients_habitat` keys of
`portal-summary_data`.

No frame table covers habitat. `pt_validate_habitat()` is a pass-through.

**Disposition: keep.** This is a seven-row enum that belongs in the frame
(see the Airtable proposals, §14), but a seven-row Google Sheet is not a
migration blocker and moving it unilaterally would mean inventing a
cross-country schema. Annotate the config: *"7-row code→label enum, no frame
equivalent; feeds `catch_habitat` on the API contract."*

---

## 5. PDS — `coasts::ingest_pds_trips()` / `ingest_pds_tracks()` / `describe_pds_tracks()` / `validate_pds_trips()`

**Jobs:** `ingest-pds-data`, `preprocess-pds-data`, `validate-pds-data`.
**Cross-country:** the two ingestion calls are identical to all three WIO
repos. `describe_pds_tracks()` and `validate_pds_trips()` are Timor-only;
`coasts::preprocess_pds_tracks()`, which all three WIO repos run, Timor does
not (see §12).

Verified from the raw trips parquet in `timor-dev` (`pds-trips__*`, 95,683
rows): **422 distinct IMEIs, all 422 in the frame's three Timor customers**
(`MAF / WorldFish`, `Traders`, `FSSP2: Traders`) — which is what the customer
filter guarantees. The 27 IMEIs recorded in the Phase 7 STATE entry as being in
no frame customer produce no rows here, exactly as expected: they are filtered
out at ingestion and are invisible downstream.

**Class: deliberate and documented. The reason still holds.**

### Table 6 — `devices` → **Keep, measured; blocked on Airtable**

The blocker replicated exactly, on 2026-08-18 data (97,377 submissions,
40,503 carrying an IMEI), by replaying `validate_this_imei()` over both lists:

| device list | n | alert 1 | alert 2 | **alert 3** | resolved IMEIs |
|---|---|---|---|---|---|
| Sheets `devices` | 595 | 51 | 0 | **824** | 38,973 |
| frame `pds_devices`, Timor customers | 457 | 51 | 0 | **1,475** | 38,322 |
| frame `pds_devices`, **all 910 rows** | 910 | 51 | 0 | **1,475** | 38,322 |

- Submissions resolved by Sheets but not by the frame: **651**.
- Submissions resolved by the frame but not by Sheets: **0**.
- Submissions where the two resolve *different* IMEIs: **0**.

**The third row is new and it sharpens the blocker.** Ignoring the customer
filter entirely and matching against every IMEI in `pds_devices` gives
*identical* results. So the 651 lost matches are not devices sitting in the
wrong customer — **they are not in the frame at all**. Adding the 27 known
IMEIs to a customer recovers the PDS-side trips (2,791 trips, 59 landing↔trip
matches, per Phase 7) but recovers **none** of the 651 survey-side matches.

Measured gap, both directions:

- Sheets IMEIs absent from `pds_devices` entirely: **144** — and the same 144 are
  absent from Timor's three customers, so not one of them is sitting in another
  country's customer either.
- IMEIs in Timor's frame customers absent from the Sheets: **6**. So the frame
  is **no longer a strict subset** of the Sheets — CLAUDE.md and the Phase 5
  STATE entry both say it is, and that is now stale. (None of the 6 appears on
  a survey, which is why the resolved count does not move.)

**Disposition: keep.** Config annotation should read: *"Sheets 595 IMEIs vs the
frame's 457. Switching moves alert 3 from 824 to 1,475 and strips the resolved
`tracker_imei` — hence the matched trip — from 651 submissions. 144 Sheets
IMEIs are absent from `pds_devices` altogether; the customer filter is not the
cause. Gated on Airtable, not on code."*

---

## 6. `merge_landings()`

**Job:** `merge-landings`, step 1. Reads the two preprocessed parquets plus
`timor-landings-v1-frozen__*.parquet` and concatenates.

Measured widths on the survey path:

```
timor-landings-v2_raw                  311 cols
timor-landings-v3_raw                  275 cols
timor-landings-v2_preprocessed          90 cols   (51 raw passthrough)
timor-landings-v3_preprocessed          89 cols   (50 raw passthrough)
timor-landings-v1-frozen                82 cols   (43 raw passthrough)
timor-landings-merged                   98 cols   (59 raw passthrough)
timor-landings-merged_weight           106 cols   (59 raw passthrough + 7 nutrients)
timor-landings-merged_validated_long    40 cols   (0 raw passthrough)
```

**The production freeze is still outstanding.** `timor-landings-v1-frozen*`
exists in `timor-dev` only (5 versions, newest `2814dff`, 2026-08-10). Phase 11
still gates on running `data-raw/freeze-landings-v1.R` against `production`.

---

## 7. `merge_landings()` → `calculate_weights()`

**Job:** `merge-landings`, step 2.
**Cross-country:** Mozambique has the same `model-taxa.R` shape; the weight
path was aligned in Phase 4 and delegates to
`coasts::get_taxa_morphometrics()`.

### L2 — the raw-column passthrough, and its one live reader

The prompt asks "whether Timor's raw-column passthrough superset is still
carried anywhere after Phase 5 moved validation onto standard names." It is,
and it is load-bearing.

**59 raw KoBo columns** ride from preprocessing through `merge_landings()` and
`calculate_weights()` and are dropped at `validate_landings()`. They include the
whole `_validation_status.*` block, `_geolocation.*` twice under three
spellings, `photo`, `meta/deprecatedID`, `trip_group/scan_barcode`, and both
Tetum free-text "other" fields.

Grepped for every one of them across `R/` outside the three preprocessing
files: the only hit is `format-public-data.R:949`, and that is a *rename
target* (`happiness_rating = happiness`), not a read.

**But `inst/report/enumerators_summary.Rmd` reads nine of them**, off
`get_weighted_landings()`:

```r
landing_id        = `_id`
landing_site      = landing_site_name
data_activity     = Ita_koleta_dadus_husi_atividad
n_boats           = no_boats
reason_zero_boats = reason_no_activity
date, today, start, end
```

and `generate_enumerators_report.R` is the **last step of the active
`export-trips` job** — it runs on every pipeline run. So the superset is not
dead weight; it is the enumerators report's only input path.

Cost, measured: `timor-landings-merged` and `timor-landings-merged_weight` are
the two largest prefixes in `timor-dev` at **0.81 GB and 0.87 GB** across 40
and 35 versions — roughly 20 MB and 25 MB per run, of which the majority is the
59 columns. Total bucket: 4.78 GB over 581 objects.

**Class: deliberate but undocumented.** It should be documented, not removed:
the nine columns the report needs must survive, and the argument for
`select()`ing down to those nine plus the standard set is a size argument only.

**Disposition:** add a line to `CLAUDE.md` and to `merge_landings()`'s roxygen
saying the passthrough exists for `enumerators_summary.Rmd` and naming the nine
columns. If the superset is ever narrowed, narrow it to those nine. **Phase 11
must not delete the passthrough**, which the current AUDIT wording ("they go
with the rest of the legacy passthrough in Phase 11") implies it may.

### Table 10 — `morphometric_table` → **Keep. The target is the hub, and the hub has no home for it yet**

It is **live**, not a leftover:
[`calculate_weights()`](../../R/model-taxa.R#L33) reads it and passes it to
`get_morphometric_tables(conf, manual_table = metadata$morphometric_table)`,
which `bind_rows()`es it onto the FishBase/SeaLifeBase fetch *before*
`summarise_lw_coeffs()` aggregates.

Measured content — and it is not what the COASTS-TODO acceptance gate
describes:

| | value |
|---|---|
| rows | 559 |
| distinct `interagency_code` | **11** — `COZ CRA CUX FLY GZP IAX MOO OCZ PEZ SFA SLV` |
| distinct `Species` | 98 |
| rows with usable `a` and `b` | 559 (all) |
| mean `a` / mean `b` | 0.4448 / 2.803 |
| `Type` distribution | CW 128, CL 119, TL 197, ML 60, ShL 22, BL 18, others 15 |
| `aL`/`bL` non-`NA` | **0** |

The gate numbers in COASTS-TODO (693 species, 5,926 rows, mean `a` 0.02716,
mean `b` 2.9781) describe the **coasts FishBase fetch**, not this table. This
is a curated, mostly-invertebrate supplement: nine of the eleven codes are
molluscs, crustaceans, echinoderms or seaweed, exactly the taxa FishBase does
not carry.

Weight at stake, from the current weighted artefact (4,993.1 t total):

| code | kg | % of national catch weight |
|---|---|---|
| FLY | 81,676 | 1.636 |
| OCZ | 65,642 | 1.315 |
| GZP | 26,302 | 0.527 |
| MOO | 19,750 | 0.396 |
| COZ | 3,463 | 0.069 |
| SLV | 3,362 | 0.067 |
| IAX | 2,188 | 0.044 |
| PEZ | 1,808 | 0.036 |
| SFA | 1,778 | 0.036 |
| CRA | 541 | 0.011 |
| CUX | 24 | 0.000 |
| **total** | **206,534** | **4.14 %** |

**Disposition: keep.** The `aL`/`bL` columns are entirely empty and
`pt_validate_morphometric_table()` coerces them for nothing — drop those two
from the parser. The correct destination is a coasts-side curated-coefficient
table, not the frame; that is a new COASTS-TODO item, filed in §14. **Do not
attempt it in Phase 11** — 4.14% of published catch weight is behind it and the
acceptance gate is "total catch weight must not move".

---

## 8. `export_api_raw()` / `export_api_validated()`

**Job:** `export-api`, gated `if: !endsWith(github.ref, '/main')`.
**Cross-country:** this is the shared contract.

Verified across all four countries' newest `-dev` validated parquets: **22
columns, identical names in identical order.** Row counts Kenya 340,398,
Timor 144,343, Zanzibar 16,404, Mozambique 2,046.

### L5 — Timor is the only country publishing a non-frame `landing_site`

Timor's `landing_site` on this contract is the **Sheets** `station_name`
(`Uaroana/Akrema`, `Beloi/Usu Bemasu`, `Seiçal/Buruma/Fatuk Bo'ot/Uai'ca`); the
frame writes `Uaroana/Acrema`, `Beloi/Usu Bemacu`, `Seical/Buruma/Fatuk
Bo'ot/Uaica'a`. The other three take it from `map_surveys()`'s
`rename(landing_site = "site")`, i.e. the frame.

Meanwhile Timor's `gaul_1_name` on the same rows **is** frame-derived —
`Lautém`, `Liquiçá`, `Oecussi` with diacritics, from
`api_submission_extras()`. So a single Timor row mixes the two vocabularies.

Mechanically: `validate_landings()` assembles
`landing_site = "station_name"` (Sheets) from `validate_sites()`, and
`api_submission_extras()` re-attaches `gaul_*` from the preprocessed frame
join. See [validation.R:154-158](../../R/validation.R#L154).

**Class: deliberate but undocumented** — the Phase 5 decision to keep
`validate_sites()` on the Sheets is on record; its consequence for the *API*
contract is not, because Phase 5 predates Phase 6.

**Disposition:** document it now; resolve it with `stations` (§10). It does not
break the schema and no consumer has complained, but a cross-country consumer
joining Timor's `landing_site` to the frame will miss on 11 of 40 sites.

---

## 9. `merge_trips()`

**Job:** `merge-trips`.

### L1 — the recorded reason is false: all three WIO repos have this function

`CLAUDE.md` states, twice, that `all_trips__*.rds` "has **no cross-country
counterpart** — no other country has a `merge_trips()` at all", and Phase 8
used that to justify leaving it as nested `.rds` while the rest of the path
moved to parquet.

Measured:

| repo | function | file | algorithm |
|---|---|---|---|
| **Mozambique** | `merge_trips()` | `R/merge-trips.R:20` | `(landing_date, imei)` + `unique_trip_per_day` split + `full_join` + `bind_rows` |
| **Zanzibar** | `merge_trips(site=)` | `R/merge-trips.R:648` | same core, plus a device-registry pre-match |
| **Kenya** | documented in `R/match-trips.R:285` | | same — "Matched records: Survey-trip pairs where both have `unique_trip_per_day = TRUE`" |
| **Timor** | `merge_trips()` | `R/merge-trips.R:16` | same |

Mozambique's is **line-for-line Timor's**, including the comments — "We assume
the landing date to be the same as the date when the trip ended" and the
"Merging datasets datasets..." typo. The only differences are the join column
name (`imei` vs `tracker_imei`) and that **Mozambique writes parquet**
(`coasts::upload_parquet_to_cloud()`) where Timor writes gzipped `.rds`.

`coasts::merge_survey_trips()` doing a different job (COASTS-TODO C10) is
correct and unaffected; the mistake is inferring from that that no country
implements Timor's job. Three do.

**Class: unexamined drift.** Two consequences:

1. **`merge_trips()` is a genuine upstreaming candidate** — the same function
   in four repos, three of them identical in substance. It should have been on
   the Phase 10 list. File as a new COASTS-TODO (§14).
2. **The Phase 8 rationale for keeping `all_trips` as `.rds` does not hold as
   written.** The stated reason was "flattening it would buy no harmonization".
   It would buy exactly the harmonization Mozambique already has. The *other*
   half of the Phase 8 reason — that `format-public-data.R`, `model-fishery.R`
   and `pds-maps.R` all read the nested shape against a live portal — is real
   and is enough on its own. **Do not flatten it in Phase 11**; restate the
   reason correctly.

Current artefact: 176,319 rows × 26 columns, `landing_catch` a list column,
0.38 GB across 29 versions.

---

## 10. `validate_landings()`

**Job:** `validate-landings`. Runs 16 validators and
`test_validated_landings.R`.
**Cross-country:** Timor's validation is the deepest of the four. Decided in
Phase 10 to keep. **No change.**

This step reads four Sheets tables: `devices` (§5), `stations`,
`reporting_unit` and `conservation`.

### Tables 7 & 8 — `stations` / `reporting_units` → **Keep, and the reason is smaller than recorded**

[`validate_sites()`](../../R/validation-functions.R#L643) inner-joins
`stations` to `reporting_unit` on `reporting_unit` and emits `station_name` +
`reporting_region`. [`validation.R:154-157`](../../R/validation.R#L154) then
publishes them as `landing_site` and `municipality`. These are the **published**
labels, not lookups.

**`stations` (40 rows) vs the frame's 40 Timor sites** — codes match 40/40, zero
orphans either way. **11 of 40 names differ**, and every one is a real
difference, not a spelling variant:

| code | Sheets | frame |
|---|---|---|
| 1 | Adara/Atekru | Adara/Atecro |
| 2 | Beloi/Usu Bemasu | Beloi/Usu Bemacu |
| 4 | Vemasse | Vemasse/Cairavela/Kp. Baru/Edang/Sukaer Laran |
| 6 | Uaroana/Akrema | Uaroana/Acrema |
| 8 | Tutuala | **Tutuala/Valu/Savirara** |
| 10 | Beacou/Sulilaran/Palaka | Beacou/Sulilaran |
| 27 | Beasu - Viqueque | Beasu |
| 28 | Oebone/Sakato/Bausiu/… | Bausiu/Bokos/Posto Sika/Oebau/Mahata/Tulaika/Sakato- RAEOA |
| 29 | Comando | **Comando/Obrato/Behau** |
| 42 | Beto Tasi/…/Kampung Alor | Beto Tasi/…/Kampur Alor |
| 44 | Seiçal/Buruma/Fatuk Bo'ot/Uai'ca | Seical/Buruma/Fatuk Bo'ot/Uaica'a |

**`reporting_units` (17 rows) vs the frame's `geo.gaul_1_name`** — 40 of 40
sites resolve a `gaul_1_name`; **22 agree**. The recorded number is "18 of 40
disagree", which is true but is three different things, and only one of them is
a problem:

| kind | n | detail |
|---|---|---|
| **Atauro** | **8** | Timor reports Atauro as its own unit; GAUL nests it in Dili. A genuine semantic difference — and the frame supports it, since `gaul_2_name = "Atauro"` |
| **diacritics only** | **9** | Lautem/Lautém (5), Liquica/Liquiçá (2), Oecusse/Oecussi (2) |
| **real conflict** | **1** | code 33 "Welaluhu": Sheets says **Manatuto**, the frame says **Manufahi / Fatuberliu** |

So the reconciliation is much smaller than it looks: one Airtable data question
(Welaluhu), one accent-normalisation decision, and one deliberate
Atauro-vs-GAUL rule that already has a frame column to express it.

**The hardcoded spellings.** Fifteen literals across three files depend on the
Sheets vocabulary:

- [`format-public-data.R:785`](../../R/format-public-data.R#L785) — 5
  municipality names + 5 **landing-site** names (`Com`, `Tutuala`, `Ililai`,
  `Sentru/Liarafa/Sika/Rau Moko`, `Comando`). Two of those five are among the
  11 the frame writes differently. Verified all five are present in the current
  `all_trips` data, so the classification works today and would silently drop
  `Tutuala` and `Comando` if the source switched.
- [`export.R:377`](../../R/export.R#L377) — 6 municipality names.
- [`model-fishery.R:750`](../../R/model-fishery.R#L750) — `"Lautem"`.

**Disposition: keep both.** Config annotation: *"the published `landing_site`
and `municipality`. 11 of 40 site names differ from the frame; 15 spellings are
hardcoded in `format-public-data.R`, `export.R` and `model-fishery.R`. Only 1
of the 18 municipality disagreements is a data conflict (site 33, Welaluhu) —
the rest are 8× Atauro-vs-GAUL and 9× diacritics. Any switch must pass
`data-raw/compare-portal-json.R`."*

### Table 12 — `conservation` → **Keep; propose a frame table**

Five rows: `1 Open, 2 Shade, 3 Box, 4 Ice box, 5 Other`. Read by
[`validate_conservation()`](../../R/validation.R#L121) and published as
`catch_preservation`, then as the `conservation` key of `portal-summary_data`.
No frame equivalent. Same class as `habitat`.

**Disposition: keep**, annotate, propose upstream (§14).

---

## 11. `estimate_fishery_indicators()` → `format_public_data()` → `export_files()` → enumerators report

**Jobs:** `model-indicators`, `export-trips`.
**Cross-country:** Timor-only, by PLAN §2.1 and §2.3. The WIO equivalent is
`coasts::summarize_data()` → `generate_fleet_analysis()` → `export_portal()`,
which Timor does not run (§12).

### Table 5 — `registered_boats` → **MOVE to the frame. `geo.total_boats` already holds it**

This is the one table whose disposition changes.

[`estimate_fishery_indicators()`](../../R/model-fishery.R#L32) reads
`get_preprocessed_sheets(conf)$registered_boats` and passes it to
`run_estimations()`, which does
`catch = landing_catch * n_landings_per_boat * n_boats` — **strictly linear in
`n_boats`**. It is the raising factor for every published municipal and
national estimate.

The frame's `geo` table carries `total_boats`, populated for exactly **12**
Timor `gaul_2` rows — one per reporting unit, with Atauro split out from Dili,
which is precisely Timor's reporting geography:

| reporting unit | Sheets `n_boats` | frame `geo.total_boats` | gaul_2 |
|---|---|---|---|
| Bobonaro | 506 | **506** | Atabae |
| Liquica | 683 | **683** | Bazartete |
| Dili | 365 | **365** | Cristo Rei |
| Atauro | 348 | **348** | Atauro |
| Baucau | 433 | **433** | Baucau |
| Lautem | 250 | **250** | Iliomar |
| Manufahi | 223 | **223** | Alas |
| Ainaro | 50 | **50** | Hatu-Udo |
| Covalima | 255 | **255** | Suai |
| Oecusse | 263 | **263** | Nitibe |
| **Manatuto** | **283** | **213** | Barique/Natarbora |
| **Viqueque** | **213** | **207** | Uato-Lari |
| **total** | **3,872** | **3,796** | |

**Ten of twelve are byte-identical.** Two disagree, and the pattern is
suspicious: the frame's Manatuto (213) equals the Sheets' Viqueque (213), which
is what a transposed pair looks like.

**This is not a free swap.** Applying the frame values to the currently
published `portal-municipal_aggregated`:

| region | share of national catch | factor | change |
|---|---|---|---|
| Manatuto | 18.54 % | 213/283 = 0.753 | **−24.7 %** |
| Viqueque | 5.55 % | 207/213 = 0.972 | **−2.8 %** |
| **national** | | | **−4.74 %** |

Manatuto is the **second-largest** municipality by catch. A −4.74% move in
national published catch is not a rounding difference.

The strongest argument for the move is that this is what the standard already
does: [`coasts::generate_fleet_analysis()`](../../../peskas.coasts/R/model-fishery.R#L450)
builds its `boat_registry` as `assets$geo |> select("gaul_2_name",
"total_boats")`. `geo.total_boats` **is** the cross-country registered-boat
field, and Timor's is already populated.

**Disposition: move.** **User decision, 2026-08-18: Airtable is authoritative.**
So the Sheets' Manatuto 283 and Viqueque 213 are wrong, and the live portal is
currently over-reporting — this is a correction to publish, not a harmonization
nicety.

The swap was verified end to end against the live snapshot. It needs a
**four-case recode**, not an accent strip: `iconv(..., "ASCII//TRANSLIT")` yields
`Laut'em` / `Liquic'a` on macOS and matches nothing.

```r
frame_boats <- timor_assets(get_assets(conf)$geo, conf) |>
  dplyr::filter(!is.na(total_boats)) |>
  dplyr::transmute(
    reporting_region = dplyr::case_when(
      gaul_2_name == "Atauro"  ~ "Atauro",     # Timor's own reporting unit
      gaul_1_name == "Liquica" ~ "Liquica",
      gaul_1_name == "Lautem"  ~ "Lautem",
      gaul_1_name == "Oecussi" ~ "Oecusse",
      TRUE ~ gaul_1_name
    ),
    n_boats = as.integer(total_boats)
  )
```

(the two accented literals above are `Liquica` and `Lautem` with their diacritics
— write them from the snapshot, not from this page.)

Verified with that recode: the 12 `reporting_region` keys are **`identical()`** to
the Sheets table's, all **12 of 12** `municipality` values in `all_trips` resolve,
and only the two intended values move. National `n_boats` 3,872 -> 3,796
(**-1.96%**); national published **catch -4.74%**, the larger figure because catch
is raised per municipality and Manatuto carries 18.54% of it.

**This does not depend on the full site/label reconciliation.** It needs only the
Atauro rule and three municipality spellings — 4 lines — not the 40-site
`landing_site` work. So it can land independently of, and before, item 5 of
Phase 12.

### L3 — two different "North Coast" definitions in one published object

Both of these write into `portal-summary_data`:

```r
# format-public-data.R:785  → summary_data$n_surveys
municipality %in% c("Bobonaro", "Liquica", "Dili", "Baucau", "Oecusse")   # 5

# export.R:377              → summary_data$estimated_revenue
region %in% c("Oecusse", "Bobonaro", "Liquica", "Dili", "Manatuto", "Baucau")  # 6
```

**Corrected 2026-08-18 after measuring at site level. An earlier draft of this
section said the two rules "disagree on Manatuto". That is the minor half of the
disagreement and it misidentifies which rule is wrong.**

The two rules are not competing definitions of equal standing.
`format-public-data.R` has a **second, site-level clause** — the five landing-site
names — and `export.R` does not, because by the time `export_files()` runs the
data has already been collapsed to municipality by the model and the site is
gone. So one rule can see the coast and the other cannot.

**Coast is a property of the site, not of the municipality.** Measured over the
40 sites and the 76,379 landings in the current `all_trips`: exactly **two**
municipalities contain sites on both coasts, and both are lopsided to the point
of being effectively single-coast for sampling purposes.

| municipality | North subs | South subs | % north | sites |
|---|---|---|---|---|
| **Lautem** | **4,403** | **2** | 99.95 % | N: Com 2,059 · Tutuala 1,514 · Ililai 827 · Sentru/Liarafa 3 — S: Lore 2 |
| **Manatuto** | **2,919** | **1** | 99.97 % | N: Comando 2,919 — S: Welaluhu 1 · Natarbora 0 |

Every other municipality is pure. So the submission counts settle it: neither is
a real split.

**The six sites the two rules classify differently:**

| site | municipality | subs | `format_public_data()` | `export.R` |
|---|---|---|---|---|
| Com | Lautem | 2,059 | North | **South** |
| Tutuala | Lautem | 1,514 | North | **South** |
| Ililai | Lautem | 827 | North | **South** |
| Sentru/Liarafa/Sika/Rau Moko | Lautem | 3 | North | **South** |
| Welaluhu | Manatuto | 1 | South | **North** |
| Natarbora | Manatuto | 0 | South | **North** |

**4,402 submissions net, and 4,403 of them are Lautem.** Manatuto contributes
**one**. `export.R`'s `"Manatuto"` entry is a crude compensation for not being
able to see Comando; what it is actually missing is **Lautem**.

**`format_public_data()` is the correct rule.** `export.R` misfiles Lautem's
**14.25 %** of national revenue into South Coast. Measured against the newest
published `portal-municipal_aggregated`:

| Area | as published today | with Lautem in North |
|---|---|---|
| Atauro island | 13,369,136 (7.9 %) | 13,369,136 (7.9 %) |
| North Coast | 109,079,352 (**64.8 %**) | 133,087,662 (**79.0 %**) |
| South Coast | 45,995,276 (**27.3 %**) | 21,986,965 (**13.1 %**) |

A 14-point error on a headline portal figure.

**Latitude cannot be used to derive this, verified.** The frame now carries
lat/lon for all 40 Timor sites, but the two classes overlap: north-coast sites
span −9.310 to −8.358 and south-coast sites −9.350 to −8.645. Oecusse is an
exclave whose north-facing sites sit at −9.31/−9.20, further south than most of
the south coast. A latitude threshold misclassifies it.

**Class: unexamined drift, and a live bug.** Both lines predate the migration.

**Disposition, in two parts:**

1. **Immediate, one word:** add `"Lautem"` to `export.R:377`. The two rules then
   agree on all but **3** of 76,379 landings (Welaluhu 1, Natarbora 0, Lore 2).
2. **Durable:** coast is a 40-row site attribute. Put the site→coast assignment
   in one place — an `inst/config.yml` list, or better a `coast` field on the
   frame's `landing_sites`, which the other three countries could use too — have
   `format_public_data()` read it instead of five hardcoded site names, and
   derive `export.R`'s municipality list from it by majority. That removes the
   duplicate rule rather than synchronising two copies of it.

Gate either on `data-raw/compare-portal-json.R`. **Not Phase 11** — part 1 alone
moves a published number by 14 points.

**User decision, 2026-08-18: fix it, "for completeness and correctness".** Part 1
lands as its own commit with its own dev run and portal diff, before the Phase 11
deletions, so the 14-point move is attributable to one change. Part 2 stays in
Phase 12 with the rest of the label work.

### The seven portal objects

Confirmed against `public-timor-dev`: the seven live objects are present and
current (`0573d4f`, 2026-08-14). `portal-indicators_grid` and
`portal-label_groups_list` are still in the bucket at their pre-Phase-8
versions (2026-08-12) and are excluded by the portal's `fetchData.js`, exactly
as documented.

---

## 12. The standard steps Timor does not run

Timor's DAG ends at `export_files()`. All three WIO repos continue:

```
coasts::preprocess_pds_tracks   →  <pds_tracks.file_prefix>-grid_summaries   [country bucket]
                                        ↓
coasts::summarize_data          →  <surveys.summaries.file_prefix>_{monthly_summaries,
                                      all_monthly_summaries, taxa_summaries, …}   [hub]
                                        ↓
coasts::generate_fleet_analysis →  <country>_fishery_metrics                  [hub]
                                        ↓
coasts::export_portal           →  MongoDB, the multi-country portal
```

### L9 — `coasts::summarize_data()`, re-examined as the prompt asks

Addendum 5 and the C17 withdrawal are correct and the reason still holds, but
the prerequisite list is now exact. Reading coasts' `R/summarize-data.R`, the
function takes three inputs:

| input | resolves | Timor status |
|---|---|---|
| `asfis` | `resolve_storage_opts(conf, "country")` | **absent** — `timor-dev` has 0 `asfis*` objects (and so does `peskas-coasts-dev`; it is a per-country object, as the C17 correction found) |
| the API validated parquet, `conf$api$trips$validated$file_prefix` | `resolve_storage_opts(conf, "api", error_if_missing = TRUE)` | **present** — `peskas-api-dev/timor/validated/`, 144,343 rows, 22 columns |
| `paste0(conf$pds$pds_tracks$file_prefix, "-grid_summaries")` | `country_opts` | **absent** — 0 in `timor-dev`; requires `preprocess_pds_tracks()` |

and writes to `resolve_storage_opts(conf, "coasts")`.

Config gaps confirmed: `conf$surveys$summaries` is **`NULL`** and
`conf$surveys$aggregated` is **`NULL`**. `conf$pds$pds_tracks$file_prefix`
(`"pds-tracks"`) and `conf$api$trips$validated$file_prefix` are both present and
correct.

So the four prerequisites are: seed `asfis` into `timor-dev`, declare
`surveys.summaries.file_prefix`, declare `surveys.aggregated.file_prefix`, and
run `preprocess_pds_tracks()`.

**The decision does not change, and the reason is still "no Timor consumer".**
`timor-dev` holds none of these outputs; `peskas-coasts-dev` holds 210
grid-summary, 401 monthly-summary and 256 fishery-metrics objects for the other
three. Running the chain would produce ~1.4 M grid rows per run for nothing.
COASTS-TODO C20 (`detectCores() - 1` on a CI runner) remains a secondary cost,
not the reason.

**Class: deliberate. Verified, reason holds.**

### L4 — `coasts::generate_fleet_analysis()` has never been examined

All three WIO repos call it, between `summarize_data()` and `export_portal()`.
It appears **once** in Timor's entire migration corpus — one cell of one table
in `PLAN.md:43` — and nowhere in `CLAUDE.md`, `STATE.md`, `AUDIT.md`,
`STRUCTURAL-DIFF.md` or `COASTS-TODO.md`. No phase has assessed it.

What it does, from `peskas.coasts/R/model-fishery.R:450`: filters
`assets$devices` to `conf$pds$customers`, takes `assets$geo` for the
`gaul_2_code`s those devices sit in, builds `boat_registry` from
`geo.total_boats`, computes monthly trip statistics from raw PDS trips,
estimates fleet activity against the registry, and joins
`<summaries>_monthly_summaries` to produce district totals and an annual
summary.

**This is the standard's version of `estimate_fishery_indicators()`.** It raises
sampled catch to a fleet total using a registered-boat count — the same job, the
same input field (`geo.total_boats`, §11), a different estimator (PDS trip rates
rather than glmmTMB).

**Class: unexamined drift.** It does not need to be adopted — PLAN §2.3 keeps
Timor's modelling, and this function is downstream of `summarize_data()` which
Timor does not run. But "we compared Timor's modelling against the standard's"
has never actually been done, and this is the function it would be compared
against.

**Disposition:** document the comparison and the decision. This is a
paragraph of writing, not code. It belongs in the proposed Phase 12.

### `coasts::export_portal()`

Not run, by PLAN §2.1 — the portal contract is the seven `public-timor`
JSONs. Requires `surveys.summaries.file_prefix` and
`surveys.aggregated.file_prefix`, both absent. **Deliberate and documented, no
change.**

### C21 — the secret leak is still live in the other three repos

None of the three WIO repos passes `log_threshold = logger::INFO` to any
`coasts::` workflow call. Timor's `data-pipeline.yaml` passes it on both PDS
steps. Unchanged since Phase 7; still worth raising with those repos' owners.

---

## 13. What Phase 11 may and may not delete

The audit's practical output. **Phase 11 may delete:**

- `metadata.google_sheets.tables`: `vms_installs`, `centro_pescas` — no reader
  anywhere.
- `pt_validate_vms_installs()`, `pt_validate_centro_pescas()`, and the
  `device_installs` / `centro_pescas` list elements in
  `preprocess_metadata_tables()`.
- The `centro_pescas` comment in `inst/config.yml:310`, which is factually
  wrong.
- `get_preprocessed_metadata()` (duplicate of `get_preprocessed_sheets()`),
  `inst/report/unanswered_summary.Rmd`, `inst/report/generate_form_summary.R`.
- The `aL` / `bL` coercion in `pt_validate_morphometric_table()` — both columns
  are 100% `NA`.
- `boats` / `fishing_vessel_statistics` **only if** the boats section of
  `data_report.Rmd` goes with them.

**Phase 11 must NOT delete:**

- **The 59-column raw passthrough.** `enumerators_summary.Rmd` runs on every
  pipeline run and reads nine of them. The current AUDIT wording implies Phase
  11 removes it; it must not.
- `devices`, `stations`, `reporting_units`, `catch_types`,
  `morphometric_table`, `habitat`, `conservation`, `registered_boats` — all
  have live readers.
- `catch_types.interagency_code` — a live tinytest assertion.

**Phase 11 must NOT change:**

- `timor_assets()`'s filter key. The planned swap onto `country` breaks `sites`
  and `geo` (§2).
- `all_trips__*.rds` to parquet. The stated reason was wrong (§9) but the real
  reason — three readers against a live portal — stands.
- The `registered_boats` source. −4.74% of published national catch (§11).

---

## 14. Airtable data-entry tasks for the user

**Read-only session — none of this was created, renamed or populated.** All of
it is data entry in `PESKAS | FRAME` (`appMMEJYlJdfSJEjm`), none of it is code.

### A. `pds_devices` — 144 missing Timor devices *(blocks table 6)*

Add the **144** IMEIs that appear in Timor's Sheets `devices` table but in no
`pds_devices` row, with `customer_name` set to one of `MAF / WorldFish`,
`Traders`, `FSSP2: Traders`. This is what unblocks switching `validate_imeis()`
off the Sheets; until it is done, the switch costs 651 resolved trips and moves
alert 3 from 824 to 1,475.

Separately, the **27** IMEIs from the Phase 7 STATE entry that produce trips but
sit in no frame customer need a `customer_name`. **Note that these are two
different jobs** — measured this session, matching against all 910 `pds_devices`
rows rather than just Timor's customers gives *identical* survey-side results,
so fixing the 27 recovers PDS trips but **zero** of the 651 survey matches.

Also worth reconciling: **6** IMEIs are in Timor's frame customers but not in the
Sheets. None appears on a survey, so nothing depends on them, but the frame is
no longer the strict subset the docs claim.

### B. `geo.total_boats` — reconcile 2 of 12 values *(blocks table 5)*

| gaul_2 | gaul_1 | frame says | Sheets say |
|---|---|---|---|
| Barique/Natarbora | Manatuto | **213** | 283 |
| Uato-Lari | Viqueque | **207** | 213 |

The other ten match exactly. Which is right is a fisheries question, not a
data-engineering one, and it moves published national catch by up to 4.74%.
Note the frame's Manatuto (213) equals the Sheets' Viqueque (213), which may
indicate a transposition in one source.

### C. `landing_sites` — one municipality conflict *(informs tables 7/8)*

Site code **33, "Welaluhu"**: the Sheets `stations` table assigns it to
**Manatuto**; the frame resolves it via `gaul_2_code` to **Manufahi /
Fatuberliu**. One of the two is wrong. Low volume (1 trip in the current merged
data) but it is the only genuine municipality conflict of the 40.

### D. Two new tables — a **proposal**, not a change

Both are small code→label enums with no frame home, read by Timor and published
on the cross-country API contract. Neither should be created unilaterally;
this is a schema proposal for whoever owns the base.

**`habitats`** — feeds `catch_habitat`, one of the 22 API columns.

| field | type | note |
|---|---|---|
| `country` | text | plain text, as on `taxa`/`gears`/`vessels` |
| `form_id` | link → `forms` | as on the other asset tables |
| `survey_label` | text | the raw form code |
| `standard_name` | text | the harmonized label |

Timor's 7 rows: `1 Reef, 2 FAD, 3 Deep, 4 Beach, 5 Traditional FAD,
6 Mangrove, 7 Seagrass`.

**`conservation`** — feeds `catch_preservation` and `portal-summary_data`.
Same four fields. Timor's 5 rows: `1 Open, 2 Shade, 3 Box, 4 Ice box,
5 Other`.

If the other three countries record equivalents, they should be gathered before
either table is created; Timor alone does not justify a cross-country table.

### E. Not an Airtable task — a coasts task

Nothing in the frame is the right home for `morphometric_table` (§7). It is 559
length-weight coefficient rows over 11 taxa, and the frame is a label-mapping
layer. The right home is a coasts-side curated-coefficient table that
`coasts::get_taxa_morphometrics()` pools automatically. Filed as a COASTS-TODO
below.

### New COASTS-TODO items this audit produces

- **C22 — upstream `merge_trips()`.** Four repos, one algorithm; Mozambique's
  and Timor's are line-for-line identical (§9). Not `merge_survey_trips()`,
  which is C10's different job.
- **C23 — a curated length-weight supplement in the hub.** Timor pools 559
  manual rows over 11 mostly-invertebrate codes with the FishBase fetch;
  4.14% of national catch weight depends on it (§7). No other country has
  anywhere to put such rows.
- **C24 — `geo.country` is a record-id link, like `landing_sites.Country`.**
  C13 fixed `taxa`/`gear`/`vessels` and explicitly skipped `sites`; `geo` was
  never checked and has the same defect (§2).

---

## 15. Sequencing recommendation

**Ordering corrected 2026-08-18 at the user's challenge, and they were right.**
An earlier draft put both production conversions first. The argument for that was
wall-clock — they are slow — presented as if it were a correctness argument. It
is not. "The first production run fails without them" establishes only that they
must precede the **merge**, not that they must come **first**.

The two scripts are different cases and only one of them is order-sensitive:

- **`freeze-landings-v1.R` must run *after* Phase 11.** It produces a
  **permanent** artefact — made once, read by `merge_landings()` forever — and it
  is built by package code: `get_preprocessed_sheets()`, `get_taxa_list()`, and
  four internals reached through `:::` (`sum_fishers()`, `mesh_size_mm()`,
  `resolve_catch_taxa()`, `resolve_survey_labels()`). Running it before a
  code-deletion phase commits a permanent production object built by code that is
  about to change: unreproducible afterwards, and silently inconsistent if Phase
  11 touches any of the six.
- **`convert-pds-tracks.R` can run at any time, and should start early.**
  Verified: it is purely additive (it writes `.parquet` and deletes nothing — the
  97,827 `.csv.gz` survive), idempotent and resumable
  (`filter(!target %in% already)`), and depends only on `read_config()` and
  `conf$pds$pds_tracks$file_prefix`, which is marked `[new]`, not `[legacy]`, so
  Phase 11 does not touch it. It is a container-format migration of existing
  points, not a derived artefact — there is no logic in it to get wrong. At ~98k
  objects it is the long pole, so run it in parallel with the code work.

Worth stating plainly, because it changes what the risk actually is: **neither
script touches the live pipeline.** `main`'s pre-migration code reads neither
`timor-landings-v1-frozen*` nor `pds-tracks_*.parquet`, so both writes are
invisible to the running production pipeline. The hazard being avoided is not
"breaking the portal" — it is "baking a permanent artefact with interim code".

**The order:**

```
in parallel, from now:   data-raw/convert-pds-tracks.R

on the branch — one commit, one dev run and one portal diff each:
  1. the coast fix (L3 part 1)   expect North/South 64.8 -> 79.0 / 27.3 -> 13.1
  2. registered_boats -> frame   expect national catch -4.74%
  3. Phase 11 deletions          expect ZERO portal change

then, from the final branch code:
  4. R_CONFIG_ACTIVE=production Rscript data-raw/freeze-landings-v1.R
  5. merge to main
  6. watch one production run
  7. re-enable the three disabled workflows
```

Steps 1 and 2 go **before** Phase 11 deliberately: it lets Phase 11's own dev run
be asserted as **zero portal change**, which is a far stronger gate on a deletion
phase than "the diff is some mixture of three changes".

**In Phase 11 — the four safe deletions** from §13, plus the corrections to
`CLAUDE.md`:

- `merge_trips()` has three counterparts, not zero (§9).
- The raw passthrough has one live reader and is not deletable (§7).
- The frame's `pds_devices` is not a strict subset of the Sheets (§5).
- `centro_pescas` contains no coordinates (§1).

These are documentation fixes that cost nothing and prevent the next session
acting on a false premise — which is how COASTS-TODO C17 nearly broke three
pipelines.

**After Phase 11 — a new phase.** See below.

**Never:**

- Switching `validate_imeis()` to `pds_devices` before task A. It is a
  651-trip regression with a 27-IMEI fix that does not fix it.
- Switching `validate_sites()` to the frame without passing
  `data-raw/compare-portal-json.R`. Fifteen hardcoded spellings across three
  files depend on the Sheets vocabulary, and two of the five hardcoded
  landing-site names are among the eleven the frame writes differently.
- Switching `registered_boats` to `geo.total_boats` before task B. −4.74% of
  published national catch.
- Reopening COASTS-TODO C14. Withdrawn deliberately, and this session confirms
  `length_type` reaches no published object.
- Flattening `all_trips__*.rds` while `format-public-data.R`,
  `model-fishery.R` and the portal are live.

### Proposed **Phase 12 — static assets and the standard's downstream half**

Runs **after** Phase 11. Phase 11 is a cutover: strip legacy keys, one green
dev run, merge to `main`. Everything in this audit that is not a pure deletion
changes a published number, and doing that inside the cutover means a portal
regression and a config migration land in the same diff with no way to bisect
them. Merge first, then move numbers one at a time against a stable `main`.

Scope, in dependency order:

1. **`registered_boats` → `geo.total_boats`** — after Airtable task B.
   Gate: `data-raw/compare-portal-json.R`, and the national catch delta stated
   in advance.
2. **The North Coast definition** (L3) — one config list, two call sites, one
   user decision. Same gate.
3. **`timor_assets()` off hardcoded record ids**, adopting Mozambique's
   `get_airtable_form_id()` (L6). Behaviour-neutral; assert 60/9/2/40/37 rows
   before and after.
4. **`devices` → `pds_devices`** — after Airtable task A. Gate: alert 3 back to
   824 and 0 lost `tracker_imei`.
5. **`stations` / `reporting_units`** — the accent decision, the Atauro rule,
   the Welaluhu conflict, and the fifteen hardcoded spellings. Largest and last.
   Gate: `compare-portal-json.R`, and the API `landing_site` column reviewed
   against the other three countries (L5).
6. **Document `generate_fleet_analysis()`** (L4) and the
   `harmonise_*` / `map_surveys` distinction (L8). Writing only.
7. File COASTS-TODO C22–C24.

Explicitly **out of scope**: `summarize_data()` and the grid summaries (L9 —
still no Timor consumer), `morphometric_table` (needs C23 upstream first), and
anything touching the seven portal objects' names or keys.

Steps 1, 2 and 4 each move a published number and each needs the user's
sign-off on the number before the change, not after.
