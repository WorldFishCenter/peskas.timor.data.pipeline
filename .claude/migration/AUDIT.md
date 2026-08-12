# Phase 0 audit — `peskas.timor.data.pipeline`

Collected 2026-07-31 against **production** (live credentials from `auth/`).
Companion to [PLAN.md](PLAN.md) and [STRUCTURAL-DIFF.md](STRUCTURAL-DIFF.md).

Everything here is measured, not inferred. Where a measurement contradicts the
plan, §7 says so explicitly.

---

## 1. KoBo assets

Server `eu.kobotoolbox.org`, basic auth.

| | v1 | v2 | v3 |
|---|---|---|---|
| env var | `KOBO_PESKAS1` | `KOBO_PESKAS2` | `KOBO_PESKAS3` |
| asset id | `aur3fK7mtJem5Cg8Wi2SPd` | `aaztUDtRzb9SpSV7i9iptb` | `aEoWV7aprG47Q4uTpaopgD` |
| form name | SSF Landings | peskAAS | PeskAAS 2 |
| deployed / active | yes / yes | yes / yes | yes / yes |
| submissions (data API) | **12,017** | **64,997** | **22,037** |
| first submission | 2017-08-06 | 2019-04-28 | 2023-07-12 |
| last submission | **2020-08-28** | **2026-07-27** | **2026-07-30** |

Submissions per calendar year, v2:

```
2019: 8,720   2020: 16,130   2021: 22,773   2022: 13,733
2023:  3,621   2024:      0   2025:      0   2026:     20
```

Recent volume:

| since | v1 | v2 | v3 |
|---|---|---|---|
| 2025-01-01 | 0 | 20 | 10,280 |
| 2026-01-01 | 0 | 20 | 2,212 |
| 2026-07-01 | 0 | 20 | 479 |

### ⚠️ v2 is not cleanly dead — it received a backfill three days ago

All 20 post-2024 v2 submissions arrived in a single batch on **2026-07-27
12:29:29–12:29:51 UTC**. Their `date` (landing date) values are **2023-04-24 to
2023-05-28** and `today` is 2023-05-09/2023-05-17 — enumerators uploading
long-held offline forms, not new fishing activity.

Consequences for **Phase 3**:

- The freeze is still the right call: v2's *content* is historical.
- But "v2 has been inactive since 2023" is false, and a freeze that assumes it
  will silently drop any future backfill.
- The freeze must record a **submission-id watermark** (max `_id` at freeze
  time), not just a date, and Phase 3 should state plainly that post-freeze v2
  backfills are dropped. If that is unacceptable, keep a low-frequency v2
  reconciliation job instead of a hard freeze.
- v1's last submission is 2020-08-28. v1 is genuinely dead; freeze it without
  reservation.

---

## 2. Cloud storage inventory

Project `peskas`, SA `data-ingestion@peskas.iam.gserviceaccount.com`. The SA can
read/write objects but **cannot** `storage.buckets.list` — bucket names must be
known in advance.

| bucket | objects | note |
|---|---|---|
| `timor` | 33,055 | live |
| `timor-dev` | **11** | see §4 |
| `pds-timor` | 97,825 | one gz CSV per trip |
| `pds-timor-dev` | 96,393 | **stale mirror**, last write 2024-07-28 |
| `public-timor` | 17,006 | live, portal source |
| `public-timor-dev` | 3,686 | stale, last write 2024-07-28 |
| `peskas-api-dev` | 87 | zanzibar / kenya / mozambique |
| `peskas-api-prod` | 727 | zanzibar / kenya / mozambique, **live**; see §6 |
| `peskas-coasts` | 4,207 | shared hub, no `timor*` objects |

**No lifecycle policy anywhere.** Every run appends a new version and nothing is
deleted.

### `gs://timor` — 38 prefixes

Live (latest = 2026-07-31, the last production run):

```
   1593  timor-landings-v2_raw__*__.csv
   1575  metadata-tables__*__.rds
   1567  metadata-tables_preprocessed__*__.rds
   1565  timor-landings-v1_raw__*__.csv
   1502  timor-landings-v1_preprocessed__*__.rds
   1501  timor-landings-v2_preprocessed__*__.rds
   1467  pds-trips__*__.csv
   1434  timor-landings-merged__*__.rds
   1421  pds-trips_preprocessed__*__.rds
   1383  pds-trips_validated__*__.rds
   1346  timor-landings-merged_validated__*__.rds
   1297  timor-landings-merged_weight__*__.rds
   1220  pds-track_preprocessed__*__.rds
   1198  rfish-table__*__.rds
   1183  pds-track-list__*__.rds
   1147  all_trips__*__.rds
   1096  model_predictions__*__.rds
   1083  timor-landings-v2_step_1_preprocessed__*__.rds
    627  validation_alerts__*__.rds
    612  timor-landings-v3_raw__*__.csv
    553  timor-landings-v3_preprocessed__*__.rds
```

Orphaned — safe to ignore, candidates for deletion at Phase 11:

| prefix | count | last written |
|---|---|---|
| `timor-landings-v{1,2,3}_raw__*__.json` | 2,934 | 2024-09-17 |
| `timor-landings-v{1,2,3}_metadata__*__.json` | 2,941 | 2024-09-17 |
| `validation-tables__*__.rds` | 733 | 2023-04-04 |
| `timor-landings-v2_validated__*__.rds` | 27 | 2021-06-28 |
| `pds-track-complete{,_trips}__*__.rds` | 42 | 2024-07-01 |
| `metadata-tables__*__.xlsx` | 2 | 2021-04-22 |
| one-offs: `asfis__*__.parquet`, `catch_timor_structured.csv`, `catch_timor_unstructured.json`, `matched_tracks_landings.zip`, `pds_full.rds`, `timor_CPUE_light.json` | 6 | — |

### `gs://pds-timor` — one prefix

`pds-track-<trip_id>__<ts>_<sha>__.csv.gz`, 97,825 objects,
2021-06-10 → 2026-07-23.

### `gs://public-timor` — two output families

**Family A — legacy `timor_*` (still written every run, latest 2026-07-31):**

```
timor_trips.rds / .tsv        timor_catch.rds / .tsv
timor_aggregated.rds          timor_aggregated-{day,week,month,year}.tsv
timor_taxa_aggregated.rds     timor_nutrients_aggregated.rds
timor_municipal_aggregated.rds / timor_municipal-aggregated.tsv
timor_municipal_taxa.rds      / timor_municipal-taxa.tsv
summary_data.rds
```

These feed `upload_dataverse()` via `get_public_files()`. The portal does not
read them.

**Family B — `portal-*.json` (103 versions each, first written 2026-01-15):**

```
portal-aggregated            portal-taxa_aggregated
portal-municipal_aggregated  portal-municipal_taxa
portal-nutrients_aggregated  portal-data_last_updated
portal-summary_data          portal-indicators_grid *
portal-label_groups_list *
```

**Stale / dead in this bucket:**

| object | last written |
|---|---|
| `indicators_gridded.rds` | **2024-07-27** (2 years stale) |
| `tracks-map.png` | **2021-12-11** |
| `kepler-pds-map.html`, `kepler_pds_map.html`, `kepler_wcs_map.html` | 2023-05-22 / undated |

**Leaked objects:** 45 objects in `public-timor` are named with an absolute local
path, e.g.
`gs://public-timor//var/folders/6s/.../T//RtmpIQHaa5/portal-json/portal-aggregated.json`
and `gs:///tmp/Rtmph2kG06/portal-json/...`. Written 2026-01-15 and 2026-01-18
from local runs — `upload_cloud_file()` was passed the full path as the object
name. Harmless (the portal's `prefix: "portal-"` filter skips them) but it
confirms `export_files()` does not normalise object names. Fix when touching
`export.R` in Phase 8.

---

## 3. Portal contract (frozen output for Phase 8)

`peskas.timor.portal.v2/scripts/fetchData.js`:

- Lists **all** objects with prefix `portal-` in `gs://public-timor`.
- Strips prefix and version → local `<name>.json`; keeps only the newest version
  of each.
- Explicitly excludes: `pars.json`, `taxa_names.json`, `var_dictionary.json`,
  **`indicators_grid.json`**, **`label_groups_list.json`**.

So `export_files()` emits **9** objects and the portal consumes **7**:

| object | consumed by portal | golden size |
|---|---|---|
| `portal-aggregated.json` | ✅ | 1,110,860 B |
| `portal-municipal_taxa.json` | ✅ | 3,252,709 B |
| `portal-municipal_aggregated.json` | ✅ | 473,456 B |
| `portal-taxa_aggregated.json` | ✅ | 390,214 B |
| `portal-nutrients_aggregated.json` | ✅ | 140,504 B |
| `portal-summary_data.json` | ✅ | 23,250 B |
| `portal-data_last_updated.json` | ✅ | 22 B |
| `portal-indicators_grid.json` | ❌ excluded | 586,438 B |
| `portal-label_groups_list.json` | ❌ excluded | 867 B |

**The contract is discovery-based, not a fixed list.** A renamed or dropped
object does not fail the build — it silently vanishes from the live site. Phase 8
must assert on names, not just on content.

**Resolved in Phase 8 (2026-08-12).** The assertion is
`data-raw/compare-portal-json.R`, which checks names first, then keys, nesting,
column sets and column types, and reports row counts and column order without
failing on them. And the emitted set is now **seven, not nine**: the two rows
marked ❌ above were dropped, because both were rebuilt on every run from an
`indicators_gridded.rds` last written 2024-07-27 (2023-05-21 in dev) by
`ingest_pds_map()`, which no workflow calls. The seven the portal consumes are
untouched. Existing `portal-indicators_grid` / `portal-label_groups_list`
versions remain in the bucket.

---

## 4. Dev environment is not a working mirror — the plan's verification strategy needs work

PLAN §5 states "Dev/prod isolation is free… use pushes as the integration test."
Measured, it is not free.

`gs://timor-dev` contains **11 objects total**, all from the 2026-07-30
`feat-upgrade` push:

```
metadata-tables, metadata-tables_preprocessed, rfish-table,
timor-landings-v{1,2,3}_raw (.csv), timor-landings-v{1,2,3}_preprocessed,
timor-landings-v2_step_1_preprocessed, timor-landings-merged
```

Nothing downstream of `merge_landings` has ever been produced in dev. That run
([30544133828](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/actions/runs/30544133828))
ended:

| job | result |
|---|---|
| Build R container | ✅ |
| Ingest metadata tables | ✅ |
| Ingest/preprocess legacy + ongoing landings | ✅ |
| Ingest-process landings 1/2, 2/2 | ✅ |
| **Ingest pds data** | ❌ `curl::curl_fetch_disk` libcurl error |
| **Merge landings** | ❌ `Error in file(file) : invalid 'description' argument`, `Calls: … get_rfish_table -> file` |
| Preprocess/validate PDS, validate landings, merge trips, model, export | ⏭ skipped |

Two independent failures:

1. `ingest_pds_tracks()` — network/libcurl failure fetching a track.
2. `calculate_weights()` → `get_rfish_table()` — `cloud_object_name()` returned
   nothing, so `readr::read_rds(file = character(0))` threw. `ingest_rfish_table()`
   is marked `continue-on-error: true` in the metadata job, so the pipeline
   proceeds without its output and fails ~50 minutes later in a different job.

**Actions:** treat "seed the dev buckets" as explicit work in Phase 1 or 2, and
drop `continue-on-error` from `ingest_rfish_table` (or make `get_rfish_table()`
fail fast with a clear message) in Phase 9. Until dev is seeded, a green
branch-push run is not achievable and cannot serve as the phase gate.

`gs://pds-timor-dev` (96,393 objects) and `gs://public-timor-dev` (3,686) both
stop at 2024-07-28 — stale, and worth a lifecycle rule or a bulk delete at
Phase 11.

---

## 5. GitHub Actions

### State (via `gh api .../actions/workflows`)

| workflow | state | last green run | verdict |
|---|---|---|---|
| `data-pipeline.yaml` | active | 2026-07-31 ✅ | **the only thing producing data** |
| `data-report.yaml` | disabled (inactivity) | 2026-06-01 | re-enable, it works |
| `check-standard.yaml` | active | none recent (fails in ~10 s) | rewrite P9 |
| `pkgdown.yaml` | active | none recent | rewrite P9 |
| `test-coverage.yaml` | active | none recent (fails in ~8 s) | rewrite P9 |
| `dataverse-upload.yaml` | disabled (inactivity) | fails ≥2026-04 | fix or retire P9 |
| `form-summary.yaml` | disabled (inactivity) | fails ≥2025-08 | **retire** |
| `upload-matched-trips.yaml` | disabled (inactivity) | fails ≥2025-08 | **retire** |
| `validation-email-sender.yaml` | disabled (inactivity) | fails ≥2025-09 | decide with P5 |
| `keplergl-map.yaml` | disabled **manually** | fails since 2026-02 | **retire** |
| `pr-commands.yaml` | active | no runs at all | retire |

Five workflows (`form-summary`, `keplergl-map`, `upload-matched-trips`,
`validation-email-sender`, and the `build-container` job in each) still push to
the **retired** `docker.pkg.github.com` registry via
`whoan/docker-build-with-cache-action@v5`. That alone explains their failures.

`validation-email-sender` has not succeeded since ≥2025-09. **The weekly
validation email has been silently dead for ~10 months.** That materially
weakens the "moving the flags sink to MongoDB requires rewriting
`send_validation_mail()`" objection in PLAN §2 — there is no working reader to
preserve. Recommendation for Phase 5 stands: **move to MongoDB**.

### Secrets

Repository secrets that exist:

```
AIRTABLE_KEY  GOOGLE_SHEET_ID  KOBO_PASSWORD  KOBO_PESKAS1  KOBO_PESKAS2
KOBO_PESKAS3  KOBO_USERNAME  PESKAS_DATAINGESTION_GCS_KEY
PESKAS_DATAVERSE_TOKEN  PESKAS_GMAIL_KEY  PESKAS_PDS_SECRET
PESKAS_PDS_TOKEN  VALID_SHEET_ID
```

Per workflow:

| workflow | secrets consumed |
|---|---|
| `data-pipeline` | `KOBO_PASSWORD`, `KOBO_USERNAME`, `KOBO_PESKAS1/2/3`, `PESKAS_DATAINGESTION_GCS_KEY`, `AIRTABLE_KEY`, `PESKAS_PDS_TOKEN`, `PESKAS_PDS_SECRET`, `VALID_SHEET_ID`, `GOOGLE_SHEET_ID` |
| `data-report` | `PESKAS_DATAINGESTION_GCS_KEY`, `AIRTABLE_KEY` |
| `dataverse-upload` | `PESKAS_DATAINGESTION_GCS_KEY`, `PESKAS_DATAVERSE_TOKEN` |
| `validation-email-sender` | `PESKAS_DATAINGESTION_GCS_KEY`, `PESKAS_GMAIL_KEY`, `VALID_SHEET_ID` |
| `form-summary`, `keplergl-map`, `upload-matched-trips` | `PESKAS_DATAINGESTION_GCS_KEY` |
| `check-standard`, `pkgdown`, `test-coverage`, `pr-commands` | `GITHUB_TOKEN` only |

Not present, needed later: `KOBO_TOKEN` (if switching to token auth),
`MONGODB_CONNECTION_STRING`, `MONGODB_CONNECTION_STRING_VALIDATION` (Phase 5).

`AIRTABLE_KEY` is passed to `data-pipeline` and `data-report` but **no R code
reads `Sys.getenv("AIRTABLE_KEY")`** — `inst/conf.yml` has no `airtable` block at
all. It is a leftover. See §7.

---

## 6. The cross-country API — live in production, Timor is the last to join

> **Correction, 2026-07-31.** The first pass of this section claimed the prod API
> bucket did not exist. That was wrong: it probed `gs://peskas-api`, which 404s
> because the bucket is named **`peskas-api-prod`**. PLAN.md had
> `peskas-api-{dev,prod}` right all along. Re-measured below.

| bucket | objects | span |
|---|---|---|
| `peskas-api-prod` | **727** | 2026-01-20 → 2026-07-31 |
| `peskas-api-dev` | 87 | 2026-02-19 → 2026-07-24 |

`gs://peskas-api-prod`, by prefix:

```
131  kenya/raw/trips-raw.parquet                 2026-02-02 → 2026-07-31
130  kenya/validated/trips-validated.parquet     2026-02-02 → 2026-07-31
122  mozambique/validated/trips-validated.parquet 2026-02-02 → 2026-07-31
120  mozambique/raw/trips-raw.parquet            2026-02-02 → 2026-07-31
115  zanzibar/raw/trips-raw.parquet              2026-01-20 → 2026-07-29
109  zanzibar/validated/trips-validated.parquet  2026-01-21 → 2026-07-29
```

`gs://peskas-api-dev`, by prefix:

```
23  zanzibar/raw/trips-raw.parquet               → 2026-07-06
22  zanzibar/validated/trips-validated.parquet   → 2026-07-06
12  kenya/raw/trips-raw.parquet                  → 2026-07-06
12  kenya/validated/trips-validated.parquet      → 2026-07-06
 9  mozambique/raw/trips-raw.parquet             → 2026-07-24
 9  mozambique/validated/trips-validated.parquet → 2026-07-24
```

So the cross-country API is **live in production for Kenya, Mozambique and
Zanzibar**, all three writing on the same cadence as their pipelines (Kenya and
Moz updated 2026-07-31, Zanzibar 2026-07-29). Timor is the **last** country to
join, not the first.

Empty `timor/`, `timor/raw/` and `timor/validated/` folder markers were created
in both buckets by the user on 2026-07-31, so the destinations exist and are
ready.

This is *better* for Phase 6 than the original reading:

- **The schema is established and observable.** Do not infer it from Moz's
  `R/api.R` alone — read an actual `mozambique/validated/trips-validated` parquet
  out of `peskas-api-prod` and match its schema exactly. Three countries already
  conform; Timor must not be the one that drifts.
- **Downstream consumers already exist.** Whatever reads `peskas-api-prod` (the
  `peskas-api` service, dashboards) will pick Timor up as soon as objects land
  there. Publish to `peskas-api-dev` first and confirm the shape before anything
  reaches prod.

~~Still to verify in Phase 6: that
`data-ingestion@peskas.iam.gserviceaccount.com` can **write** to
`peskas-api-prod`.~~ **Verified 2026-08-11: it can.** Answered without writing
anything, through the bucket `testIamPermissions` endpoint — the SA holds
`storage.objects.create`, `.delete`, `.get` and `.list` on **both**
`peskas-api-dev` and `peskas-api-prod`. Timor published to `-dev` in Phase 6;
the first prod write is a decision, not a permission.

`gs://peskas-coasts` contains no `timor*` objects, confirming Timor has never
participated in the shared hub.

---

## 7. Plan corrections

Measured facts that contradict PLAN.md / STRUCTURAL-DIFF.md. Fix these in the
plan before the phase that depends on them.

1. **Raw survey storage is already flat CSV, not "a pile of JSON".**
   PLAN §1 and Phase 3 describe raw as "per-batch `.json` files uploaded
   verbatim" and call the flattening a "semantic change". In fact
   `get_kobo_data()` ([retrieve-survey-data.R:225-231](../../R/retrieve-survey-data.R#L225-L231))
   already applies `flatten_row()`/`flatten_field()`/`rename_child()` and writes
   **one CSV per asset**; the `*_raw__*.json` / `*_metadata__*.json` objects
   stopped on 2024-09-17. The remaining Phase 3 work is CSV→parquet plus
   delegating to `coasts::get_kobo_data()` — smaller than budgeted.

2. **v2 is not inactive** — see §1. ✅ **Resolved 2026-07-31: v2 is not frozen.**
   Live forms run through the pipeline regardless of rate; only v1 is frozen.
   PLAN §2 and Phase 3 updated.

3. **Phase 7's "compatibility shim" risk is overstated.** The plan flags
   `indicators_gridded` and `tracks-map` as products `format_public_data()` /
   `export_files()` depend on. Measured: `indicators_gridded.rds` has not been
   written since **2024-07-27** and `tracks-map.png` since **2021-12-11**. The
   only downstream artefact, `portal-indicators_grid.json`, is regenerated each
   run from the two-year-old rds and is **excluded by the portal**. The shim is
   very likely unnecessary — verify at Phase 7 and delete the dependency instead
   of porting it.

4. **Dev is not a free integration test** — see §4. ✅ **Resolved 2026-07-31:**
   the user is copying the latest prod version of each prefix into `timor-dev`
   by hand. No pipeline change needed; PLAN §5 holds once that lands.

5. ~~**`peskas-api` prod does not exist**~~ — **this correction was itself
   wrong, withdrawn 2026-07-31.** The bucket is `peskas-api-prod`; I probed
   `gs://peskas-api` and read the 404 as absence. PLAN.md's
   `peskas-api-{dev,prod}` was correct. The prod API is **live for Kenya,
   Mozambique and Zanzibar** (727 objects, current to 2026-07-31), and
   `timor/{raw,validated}` markers now exist in both buckets. See the rewritten
   §6 — the practical effect is that Phase 6 has a real, observable schema to
   conform to, and a live consumer to avoid disrupting.

6. **The validation email is already dead** — see §5. Lowers the cost of the
   Phase 5 MongoDB move.

7. **`AIRTABLE_KEY` is dead in CI** — measurement correct, **conclusion
   partly wrong, corrected 2026-07-31.**

   What was measured holds: `AIRTABLE_KEY` is passed to two workflows and read
   by nothing, and `R/airtable.R` is orphaned — `ingest_validation_tables()`
   and `get_validation_tables()` read `pars$validation$airtable$*`, a key that
   no longer exists. Deleting that client is right.

   What the finding missed is that "Airtable" means two different things here,
   and the audit only looked at Timor's own. The **standard's** integration —
   the PESKAS | FRAME base read via `coasts::airtable_to_df()` /
   `ingest_assets()` — is the cross-country harmonization layer, and Timor has
   never had it. That is an **adoption**, not a deletion, and PLAN.md did not
   scope it. Now PLAN §2.5, with config in Phase 1 and ingestion in Phase 3.

   Measured 2026-07-31 in base `appMMEJYlJdfSJEjm`:

   | table | Timor-Leste rows | created |
   |---|---|---|
   | `taxa` | 57, `form_version: PeskAAS 2`, with `alpha3_code` | 2026-07-30 |
   | `gears` | 7, Tetum `original_name` → `standard_name` / `code` / `FAO_abbrev` | 2026-07-30 |
   | `vessels` | 2 | **2026-07-31 09:14** |
   | `landing_sites` | 40 | — |
   | `pds_devices` | 457 | — |
   | `countries` | `Current Form` = `aEoWV7aprG47Q4uTpaopgD` (live v3 asset) | — |

   The vessels rows post-date the first pass of this audit, which is part of
   why it read the base as irrelevant to Timor. The taxa `alpha3_code`s (TRI,
   CJX, MIL, BGX, MOJ, …) are the same codes as `models.all_taxa` in the
   config, so the frame is already aligned to Timor's vocabulary.

   Two credential facts, both blocking Phase 3:
   - `auth/airtable-key` stored the value as **`Bearer pat…`**, because
     `air_get_records()` passes it verbatim as the Authorization header
     ([airtable.R:43](../../R/airtable.R#L43)).
     `coasts::airtable_to_df()` does `paste("Bearer", token)`, so the bare
     `pat…` is required. Normalised in `.env` during Phase 1.
   - Even bare, that PAT returns `INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND`
     against both `appMMEJYlJdfSJEjm` and `/v0/meta/bases`. It needs base
     access plus the `schema.bases:read` scope. **User action.**

8. **`inst/airtable/edit-submission-link.js` is dead.** It targets
   `kobo.humanitarianresponse.info` (Timor now uses `eu.kobotoolbox.org`) and
   hardcodes the **v2** asset id. ✅ **Deleted in Phase 1**, with `R/airtable.R`
   and `ingest_validation_tables()`.

---

## 8. Live bugs found (not part of any phase's brief)

1. **Duplicate `get_preprocessed_metadata()`.** Defined in
   [get-cloud-files.R:110](../../R/get-cloud-files.R#L110) (correct — reads
   `pars$metadata$google_sheets$name`) and again in
   [validate-landings.R:373](../../R/validate-landings.R#L373) (broken — reads
   the removed `pars$metadata$airtable$name`). Collation puts
   `validate-landings.R` last, so **the broken definition wins**. Its consumer is
   `inst/report/unanswered_summary.Rmd` → `generate_form_summary.R` →
   `form-summary.yaml`, which has failed every run since ≥2025-08. Plausible root
   cause.

2. ~~**Dead helpers reading removed config keys**~~ — **fixed in Phase 1.**
   `get_validation_tables()` (`pars$validation$airtable$name`) and
   `get_preprocessed_landings()` (`pars$surveys$landings$file_prefix` — config
   has `landings_1/2/3`, no `landings`), both in `validate-landings.R`, both
   unexported and uncalled. Removed together with the rest of the orphaned
   Airtable surface rather than waiting for Phase 11.

3. **`ingest_rfish_table()` is `continue-on-error: true`** but its output is a
   hard dependency of `calculate_weights()` two jobs later. Turns a fast, clear
   failure into a slow, cryptic one — see §4.

4. **`config.yml` `local:` env declares `inherits: development`**, and no
   `development` environment exists (`inst/conf.yml:202`). Currently harmless
   because `local:` overrides everything it needs, but it is the same class of
   latent bug as Moz's `R_CONFIG_ACTIVE=dev`. Disappears in Phase 1.

5. **`export_files()` uploads with unnormalised object names** — see §2,
   "leaked objects".

---

## 9. Golden snapshot

Baseline for Phases 4, 5, 7 and 8. Stored in the gitignored
`reference/2026-07-31_90ede9a/` (105 MB). Every artefact is from the **same**
production run: 2026-07-31, commit `90ede9a`.

```
reference/2026-07-31_90ede9a/
├── timor/
│   ├── timor-landings-merged__20260731021959_90ede9a__.rds
│   ├── timor-landings-merged_weight__20260731024455_90ede9a__.rds
│   ├── timor-landings-merged_validated__20260731030322_90ede9a__.rds
│   ├── validation_alerts__20260731030414_90ede9a__.rds
│   ├── pds-trips_validated__20260731020517_90ede9a__.rds
│   ├── all_trips__20260731030623_90ede9a__.rds
│   └── model_predictions__20260731030923_90ede9a__.rds
└── public-timor/
    ├── portal-*.json                       (all 9)
    ├── timor_{trips,catch,aggregated,taxa_aggregated,
    │          nutrients_aggregated,municipal_aggregated,municipal_taxa}.rds
    └── summary_data.rds
```

### Structural baseline

| artefact | rows × cols | notes |
|---|---|---|
| `timor-landings-merged` | 97,151 × 61 | raw KoBo columns, `_attachments` + `species_group` list-cols |
| `timor-landings-merged_weight` | 97,151 × 61 | same shape |
| `timor-landings-merged_validated` | 97,151 × 19 | `landing_catch` list-col |
| `validation_alerts` | 97,151 × 7 | `submission_id, submission_date, flag_date, alert, validated, validated_when_ymd, comments` |
| `pds-trips_validated` | 84,741 × 8 | |
| `all_trips` | 174,893 × 26 | |
| `timor_trips` | 171,855 × 22 | |
| `timor_catch` | 1,721,341 × 14 | |
| `timor_municipal_aggregated` | 1,236 × 13 | |
| `timor_municipal_taxa` | 14,317 × 8 | |
| `timor_aggregated` | list: day 3,069 / week 448 / month 103 / year 9, ×15 | |
| `timor_taxa_aggregated` | list: day **0** / week **0** / month 1,339 / year 117, ×7 | day+week empty |
| `timor_nutrients_aggregated` | list: day **0** / week **0** / month 721 / year 63, ×4 | day+week empty |
| `summary_data` | 11 elements | `n_surveys, n_tracks, groups_comp, nutrients_per_catch, happiness_rating, conservation, cpue_df, timor_shape, catch_norm, catch_price_norm, nutrients_norm` |
| `model_predictions` | list: `national` (2), `municipal` (12) | |

`97,151` merged rows vs `99,051` total KoBo submissions → **1,900 rows (1.9%)
dropped** in merge. Not investigated; note it so a Phase 4 delta of that size is
not mistaken for a regression.

### Numeric baseline — `timor-landings-merged_validated`

`landing_date` spans **2015-07-07 → 2026-07-29**.

| column | NA rate | mean | median | min | max |
|---|---|---|---|---|---|
| `catch_price` | 0.176 | 34.4117 | 24.0 | 0 | 1350 |
| `trip_length` | 0.097 | 3.5628 | 3.0 | 1 | 96 |
| `mesh_size` | 0.449 | 44.9737 | 38.1 | 25.4 | 177.8 |
| `fuel` | 0.492 | 5.8034 | 5.0 | 0 | 86 |
| `n_gleaners` | 0.356 | 16.4411 | 5.0 | 0 | 200 |
| `fisher_number_man` | 0.066 | 2.0424 | 2.0 | 0 | 10 |
| `happiness` | 0.063 | 3.6654 | 4.0 | 1 | 5 |

`landing_catch` is non-empty on every row; flattening it yields **152,035** catch
rows with columns `catch_taxon, catch_use, length_type, length_frequency`.
**This 152,035 is the row count Phase 4a's long-format table must reproduce.**

### Numeric baseline — trips

| | `all_trips` | `pds-trips_validated` |
|---|---|---|
| rows | 174,893 | 84,741 |
| matched (`tracker_trip_id` present) | 84,741 | — |
| distinct IMEI | — | 438 |
| trip start range | — | 2018-07-01 → 2026-07-21 |
| `tracker_trip_duration` (s) | mean 17,993.83, median 14,005, min 1,801, max 343,795 | same |
| `tracker_trip_distance` (m) | mean 16,224.34, median 12,466, min 518, max 199,787 | same |
| **total tracked hours** | — | **418,077** |

Phase 7 parity gate: trip count `84,741` and total tracked hours `418,077`,
±1%.

### Numeric baseline — `timor_catch` (public)

| column | NA rate | mean | median | min | max |
|---|---|---|---|---|---|
| `catch` | 0.931 | 8,444.25 | 3,922.63 | 0 | 1,081,140.9996 |
| `length` | 0.004 | 34.8199 | 35.0 | 7.5 | 320 |
| `number_of_fish` | 0.930 | 111.5129 | 21.0 | 0 | 17,550 |

### Validation baseline (Phase 5 gate)

From `validation_alerts`: **12,805 of 97,151 flagged (13.18%)**, in **37**
distinct `alert` code combinations.

```
     0  84346        10  3943        17  2849        22  2571        11  2033
     3    772     10-17   133     10-22   101         4    98     10-11    93
     1     40         5    33        16    29      3-11    27         6    12
  3-10      9      3-17     9     22-16     8      3-22     6      5-22     5
  4-11      4     10-16     3      10-6     3        21     3      1-10     2
  1-17      2      1-21     2      1-22     2       1-5     2   10-5-22     2
  4-17      2      4-22     2      1-11     1        20     1       3-5     1
  5-11      1      5-17     1
```

Base codes in use: `1 3 4 5 6 10 11 16 17 20 21 22`. **Every one of these must
still be produced after Phase 5.**

---

## 10. Hygiene status

| item | status |
|---|---|
| `peskas.mozambique.data.pipeline/` in `.gitignore` | ✅ |
| `.Rbuildignore` entry anchored (`^peskas\.mozambique\.data\.pipeline/`) | ✅ |
| `.dockerignore` excludes reference tree, `auth`, `.env`, `.Renviron`, artefacts | ✅ |
| `*.parquet` in `.gitignore`; `.env*` narrowed so `.env.example` is committable | ✅ |
| `.Rbuildignore`: `^\.env$`, `^\.env\.example$`, `^\.claude$`, `^CLAUDE\.md$`, `^\.Renviron$`, `^\.venv$`, `^reference$`, `^.*\.parquet$`, `^.*\.rds$` | ✅ |
| `reference/` gitignored | ✅ |
| `inst/__pycache__/` deleted | ✅ |
| `.Rhistory` absent | ✅ |
| `CLAUDE.md` written and tracked | ✅ |
| **`ANTHROPIC_API_KEY` rotation** | ❌ **user action** |

### `ANTHROPIC_API_KEY` — verified state

The same key value (sha256 prefix `f6e01680b0251d6f`) is present in
`./.Renviron` **and** `./peskas.mozambique.data.pipeline/.Renviron`.
`~/.Renviron` does **not** contain it (it holds only `MAPBOX_PUBLIC_TOKEN`).
`git log --all -- .Renviron auth` is empty in both repos — **never committed, not
a leak.**

Rotation requires console access and cannot be done from here. Sequencing
matters: relocating an unrotated key to `~/.Renviron` just moves it. So:

1. Rotate at <https://console.anthropic.com/settings/keys>.
2. Put the new value in `~/.Renviron` only.
3. Delete the `ANTHROPIC_API_KEY` line from both project `.Renviron` files.

Left undone deliberately, flagged for the user.

`.Renviron` also still sets `R_CONFIG_ACTIVE=local`; that changes to `default` in
Phase 1 when the `local:` env is deleted.
