# Exhaustive structural diff: Timor vs the harmonized standard

Companion to `PLAN.md`. Covers **every** file in both repo roots, dotfiles included.
Compared: `peskas.timor.data.pipeline` (HEAD, branch `feat-upgrade`) vs
`peskas.mozambique.data.pipeline` (local reference copy), with
`peskas.coasts` and `inst/config_template.yml` as the normative spec.

Legend: **[P<n>]** = handled in that plan phase. **[!]** = act now, not in a phase.

---

## 0. The normative spec is already written down

`peskas.mozambique.data.pipeline/inst/config_template.yml` is the **PESKAS UNIFIED
CONFIGURATION TEMPLATE** — a fully commented cross-country spec that documents the
harmonization decisions (ingestion vs surveys split, stage naming, MongoDB layout,
airtable placement, version-field removal, dashboard db naming) and shows Zanzibar /
Kenya / Mozambique variants side by side.

**Timor's config must be derived from this template, not copied from Moz's
`config.yml`.** Timor must also ship its own `inst/config_template.yml`. **[P1]**

The template's §5 note is directly relevant: it removed the per-source
`version: preprocess: latest` field, and warns that pipelines reading
`version$preprocess` must add it back. Timor's current `conf.yml` uses exactly that
pattern on `landings_1/2/3`, `pds.trips`, `pds.tracks`, `metadata`, `validation`.
Decide per key during Phase 1 rather than dropping them silently.

---

## 1. Secrets and environment — the `auth/` → `.env` switch

### Current Timor mechanism

`inst/conf.yml` has a `local:` environment that reads 15 plaintext files via
`readLines()`, and `.Renviron` sets `R_CONFIG_ACTIVE=local`. So a developer's local
run silently uses a *different config branch* than CI.

`auth/` contents (gitignored, untracked — verified):

```
airtable-key            blastula_cred_file      dataverse-token
gcp-sa-peskas_ingestion-key.json                kobo-password
kobo-token              kobo-username           metadata-sheet-id
pds-secret              pds-token               peskas-gmail-key
peskas-timor-v1-id      peskas-timor-v2-id      peskas-timor-v3-id
valid-sheet-id
```

### Target mechanism

`.env` + `dotenv::load_dot_env()` called from inside `read_config()`; a single
`default`/`production` config pair; **no `local:` env, no `auth/`**. Local and CI
then differ only by `R_CONFIG_ACTIVE`, not by config branch.

### Migration map **[P1]**

| `auth/` file | env var | notes |
|---|---|---|
| `kobo-username` | `KOBO_USERNAME` | already used by CI |
| `kobo-password` | `KOBO_PASSWORD` | already used by CI |
| `kobo-token` | `KOBO_TOKEN` | not currently in CI env; Moz uses token auth |
| `peskas-timor-v3-id` | `KOBO_ASSET_ID_V3` | rename from `KOBO_PESKAS3` to the standard shape |
| `peskas-timor-v1-id` | `KOBO_ASSET_ID_V1` | retire after the v1 freeze **[P3]** |
| `peskas-timor-v2-id` | `KOBO_ASSET_ID_V2` | **keep** — v2 is a live source (decision revised 2026-07-31, PLAN §2 / AUDIT §1) |
| `gcp-sa-peskas_ingestion-key.json` | `GCP_SA_KEY` | full JSON as one value |
| `metadata-sheet-id` | `GOOGLE_SHEET_ID` | already in CI |
| `valid-sheet-id` | `VALID_SHEET_ID` | drops if flags move to Mongo **[P5]** |
| `pds-token` / `pds-secret` | `PDS_TOKEN` / `PDS_SECRET` | already in CI |
| `airtable-key` | `AIRTABLE_TOKEN` | CI currently calls it `AIRTABLE_KEY` — rename. **Strip the `Bearer ` prefix**: the stored value is a header, not a token (AUDIT §7.7) |
| — | `AIRTABLE_BASE_ID_FRAME` | new, PESKAS \| FRAME base `appMMEJYlJdfSJEjm` **[P1]** |
| `dataverse-token` | `DATAVERSE_TOKEN` | already referenced by conf.yml |
| `peskas-gmail-key` / `blastula_cred_file` | `PESKAS_GMAIL_KEY` | two files, one config key today |
| — | `MONGODB_CONNECTION_STRING`, `MONGODB_CONNECTION_STRING_VALIDATION` | new, if flags move to Mongo **[P5]** |

~~Also rename the GH secrets `KOBO_PESKAS1/2/3` → `KOBO_ASSET_ID_V1/2/3` and
`AIRTABLE_KEY` → `AIRTABLE_TOKEN`.~~ **Resolved in P9, 2026-08-12, and only
half of it happened.** `AIRTABLE_TOKEN` has existed since P1 and is what the
pipeline reads; `AIRTABLE_KEY` is verified dead on `origin/main` too (passed as
`env:`, read only by the deleted `air_*` client, whose caller is in no workflow
step and whose config block does not exist) and is the user's to delete. The
KoBo trio was **declined by the user**: secret values are write-only, so
renaming means re-entering three asset ids by hand for no behavioural gain, and
`env:` keeps `KOBO_ASSET_ID_V<n>: ${{ secrets.KOBO_PESKAS<n> }}`. The legacy
names retire in P11 with the config keys that read them. `VALID_SHEET_ID` is no
longer passed by any workflow.

Ship `.env.example` with every key and no values. **[P1]**

### `.Renviron` **[!]**

Both repos have an untracked `.Renviron` containing the **same live
`ANTHROPIC_API_KEY`** — it has been copied between projects. Verified: not tracked
and not in git history in either repo, so this is not a leak, but a key duplicated
across working trees is worth rotating and replacing with a single user-level
`~/.Renviron`.

Also: Timor sets `R_CONFIG_ACTIVE=local`, which stops resolving once the `local:`
env is deleted → change to `default`. Moz sets `R_CONFIG_ACTIVE=dev`, and **no `dev`
environment exists in its `config.yml`** — a latent bug in the reference. Do not
copy it.

---

## 2. Ignore files

### `.gitignore` **[P1]**

Timor is missing, relative to the standard:

```
*.parquet          # ← critical: the whole migration moves to parquet
*.geojson
/.quarto/
```

Timor has `.env*`, which correctly covers `.env` but **also blocks `.env.example`**,
which must be committed. Change to the Moz form:

```
.env
.env.local
.env.*.local
```

Moz additionally ignores `CLAUDE.md`. **Recommend Timor tracks it** — it is the
bootstrap file every migration session reads, and it belongs in the repo.

Evidence this matters: Moz has leaked local run artifacts into its own repo root —
`assets__20260319015206_4b9c55b__.rds`,
`wf-surveys_gleaning-validated__20260625170044_6add79b__.parquet`, and a whole
`mozambique/validated/trips-validated__*.parquet`. Timor's ignore rules should
prevent that class outright.

### `.Rbuildignore` **[P1]**

Missing entries: `^\.env$`, `^\.env\.example$`, `^\.claude$`, `^CLAUDE\.md$`,
`^\.Renviron$`, `^.*\.parquet$`, `^.*\.rds$`, `^reference$` (the golden-snapshot dir),
`^\.venv$`.

The line just added for the reference copy is a plain string, but every other line in
the file is an anchored regex. Make it consistent:

```
^peskas\.mozambique\.data\.pipeline/
```

Do not copy Moz's `.Rbuildignore` verbatim — it contains
`^peskas\.kenya\.data\.pipeline\.Rproj$` inside the *Mozambique* repo, a copy-paste
error that leaves its own `.Rproj` un-ignored.

### `.dockerignore` **[!] — act now**

Timor's is two lines (`.github`, `.git`). Moz has none at all.

`.gitignore` has no effect on the Docker build context. `Dockerfile.prod` ends with
`COPY . /home`, so a **local** `docker build` / `docker compose build` today copies
the entire 21 MB `peskas.mozambique.data.pipeline/` reference tree — including its
`.env` with live Mozambique credentials (KoBo token and password, two MongoDB
connection strings, GCP service-account key, Airtable token, PDS token and secret)
— into an image layer.

CI is unaffected: `actions/checkout` only materialises tracked files, so the GHCR
image built by the workflow does not contain it. The exposure is local builds only.

Fix now:

```
.git
.github
.Rproj.user
.venv
auth
.env
.env.example
.Renviron
docs
man
reference
peskas.mozambique.data.pipeline
*.parquet
*.rds
*.tar.gz
```

This also cuts the build context and speeds up image builds.

---

## 3. `.Rprofile` **[P1]**

Timor's is **empty**. Moz's defines `use_prod()`, `use_default()`, `show_env()` and
prints an environment banner on interactive startup — a genuinely useful convention
now that `R_CONFIG_ACTIVE` is the only thing separating dev from prod. Adopt it,
retitled for Timor, and drop the `use_local()` idea since the `local:` env is going
away.

---

## 4. `R/` layout **[P2–P8]**

| Timor file(s) | → target | phase |
|---|---|---|
| `cloud-storage.R`, `get-cloud-files.R`, `google-drive.R` | deleted → `coasts::*` | P2 |
| `ingest-landings.R`, `retrieve-survey-data.R`, `ingest-metadata-tables.R` | `ingestion.R` | P3 |
| `clean-raw-data.R`, `preprocess-landings.R`, `pt_nest_species.R`, `pt_nest_attachments.R`, `merge-landings.R`, `preprocess-metadata-tables.R` | `preprocessing-surveys.R` + `survey-reshaping.R` | P4a |
| `calculate-weights.R` | `model-taxa.R` | P4b |
| `validate-landings.R` | `validation.R` | P5 |
| `validation-functions.R` | keep name | P5 |
| — (new) | `api.R` | P6 |
| `merge-trips.R` | keep name, rewrite body | P6 |
| `ingest-pds-data.R`, `retrieve-pds-data.R`, `preprocess-pds-trips.R`, `validate-pds-trips.R` | mostly deleted → `coasts::*`; keep trip validation | P7 |
| `estimate-catch.R`, `model-catch.R` | `model-fishery.R` | P8 ✅ |
| `calculate-nutrients.R` | `nutrients.R` | P8 ✅ |
| `format-public-data.R`, `export.R` | ~~`export.R`~~ **both keep their names** | P8 — **deviation** |
| `export-dataverse.R` | keep name | P8 ✅ |
| `send-email.R` | `reports.R` | P8 ✅ |
| `airtable.R` | **deleted** — orphaned `air_*` client; the frame integration is delegated to `coasts::*` | P1 |
| `utils.R`, `utils-pipe.R`, `utils-tidy-eval.R`, `globals.R` | keep | P1 |
| — (new) | `peskas.timor.data.pipeline-package.R` | P1 |

**Deviation taken in P8, 2026-08-12:** `format-public-data.R` and `export.R`
were **not** merged. The table above is the only place the merge was ever
specified — PLAN's own Phase 8 bullet does not ask for it — and concatenating
them produces a 1,636-line `export.R` for no structural gain. The standard's
`export.R` is the portal export, which Timor's already is; the 1,200 lines of
aggregation upstream of it are a Timor-only country module and read better with
their own name. Revisit only if `format_public_data()` ever shrinks.

Timor's `airtable.R` (`air_get_records`, `air_tibble_to_records`, …) is an entirely
different API from the standard's (`airtable_to_df`, `bulk_update_airtable`,
`device_sync`, all also present in `coasts`). ~~Reconcile in P8, don't leave two.~~
**Superseded 2026-07-31.** "Reconcile" was the wrong frame — these are not two
implementations of the same thing:

- Timor's `air_*` client was orphaned and read a config key that no longer
  exists. ✅ **Deleted in P1** (pulled forward from P8), together with its only
  consumer `ingest_validation_tables()`, the two dead helpers in
  `validate-landings.R`, `inst/airtable/edit-submission-link.js`, and the
  `matches("air")` section in `_pkgdown.yml`.
- The standard's integration reads the shared PESKAS | FRAME base and is the
  cross-country harmonization layer. Timor has never had it. **Adopt it** —
  config in P1, `ingest_assets()` in P3, joins moved off Google Sheets in P4.
  Timor delegates to `coasts::` rather than vendoring a copy of the module the
  way Mozambique does, so its key paths are the hub's (`airtable.token`,
  `airtable.frame.base_id`) rather than Moz's `metadata.airtable.*`.

See PLAN §2.5 and the corrected AUDIT §7.7 for the measured Timor row counts and
the credential handling (the value must be stored bare — the standard adds the
`Bearer ` prefix itself).

---

## 5. Roxygen and pkgdown **[P9 ✅ done 2026-08-12]**

The standard drives `_pkgdown.yml` reference sections off `@keywords`:
`workflow`, `ingestion`, `preprocessing`, `validation`, `export`, `helper`, `storage`.
Timor's `_pkgdown.yml` instead uses `matches("cloud")`, `matches("get")`,
`matches("ingest")` — name-pattern matching that will silently mis-sort after the
rename. Switch to keyword sections and tag every exported function.

Do not copy Moz's `_pkgdown.yml` `url:` — it points at
`peskas.malawi.data.pipeline`. Timor's own URL is correct.

---

## 6. DESCRIPTION **[P1]**

Timor's metadata is properly filled (real Title, Description, four authors with
ORCIDs, GPL-3). **Moz's is unedited `usethis` boilerplate** — "What the Package Does
(One Line, Title Case)", `person("First", "Last", "first.last@example.com")`. Keep
Timor's; take only the dependency structure from the reference:

- lean `Imports:` + `coasts`
- `Remotes: github::WorldFishCenter/peskas.coasts` and
  `github::WorldFishCenter/ssf-ai-toolkit/Rplug@plug-R` (coasts imports `ssfaitk`)
- `Depends: R (>= 2.10)`, `LazyData: true`
- Timor keeps `glmmTMB`, `dataverse`, `blastula`, `googlesheets4`, `mongolite`

Version: Timor 3.3.0 → **4.0.0** (breaking). Moz is at 2.8.0; version numbers are
per-country, not shared.

---

## 7. Docker **[P1, P2]**

| | Timor | Standard |
|---|---|---|
| `Dockerfile.prod` base | `rocker/geospatial:4.4` | `rocker/geospatial:4.5` |
| package list | ~70 via `install2.r` | ~17 |
| `rfishbase` | pinned to 5.0.1 via `install_version` | unpinned CRAN |
| GitHub pkgs | `ggchicklet`, `glmmTMB` | `ARG COASTS_REF` → `install_github('WorldFishCenter/peskas.coasts', ref = ...)` |
| `ENTRYPOINT` | `["Rscript"]` | same |
| `Dockerfile` (dev) | `rocker/geospatial:4.4`, mirrors prod list, copies `rstudio-prefs.json` | `rocker/r-ver`, minimal, also takes `COASTS_REF`, no local install |
| `docker-compose.yaml` | present (RStudio on :8802) | absent |

The `rfishbase 5.0.1` pin is the likely conflict: `coasts` imports `rfishbase`
unpinned and uses `enrich_taxa` / `expand_taxonomic_info`. Resolve in P1 — either
drop the pin or keep Timor's taxa code off the shared helpers.

Keep `docker-compose.yaml` and the dev `Dockerfile`; they are a Timor convenience the
others lack, not drift to remove.

---

## 8. GitHub Actions

> **Done in P9, 2026-08-12.** Eleven workflows became nine:
> `check-standard.yaml` → `R-CMD-check.yaml` and `pkgdown` / `test-coverage` /
> `pr-commands` on the r-lib v2 templates with the two GitHub `extra-packages`;
> `release.yaml` added with the `vv` tag bug fixed; `form-summary`,
> `keplergl-map` and `upload-matched-trips` **deleted**;
> `validation-email-sender` rebuilt because P5 gave it a working Mongo reader;
> `data-report` and `dataverse-upload` stripped of the duplicate
> `build-container` job that had been failing since P2's `ARG COASTS_REF`.
> `data-pipeline.yaml` was **edited, not rewritten** — see the P9 STATE entry for
> the three deviations, including why Moz's job naming was not copied and why
> R-CMD-check runs one runner instead of five.

### Generic workflows — Timor is on the pre-2022 r-lib templates **[P9 ✅]**

| | Timor | Standard |
|---|---|---|
| checkout | `@v2` | `@v4` (generic) / `@v5` (pipeline) |
| setup-pandoc | `@v1` | `@v2` |
| deps | hand-rolled `remotes::dev_package_deps` + `.github/depends.Rds` cache | `r-lib/actions/setup-r-dependencies@v2` |
| runners | `macOS-latest`, `ubuntu-22.04` + focal RSPM URLs | `ubuntu-latest`, current matrix |
| `permissions:` | absent | `read-all` |
| pr-commands gating | any commenter | `MEMBER`/`OWNER` only |
| coverage upload | older covr flow | `covr::to_cobertura` + `codecov-action@v4` |
| `release.yaml` | **absent** | present |

**Hard requirement:** `coasts` is not on CRAN. Once Timor Imports it,
`R-CMD-check.yaml`, `pkgdown.yaml` and `test-coverage.yaml` all need
`extra-packages: ... github::WorldFishCenter/peskas.coasts,
github::WorldFishCenter/ssf-ai-toolkit/Rplug@plug-R` or they fail immediately.

Rename `check-standard.yaml` → `R-CMD-check.yaml` to match.

### `release.yaml` **[P9 ✅]**

Adopt it. Timor's `NEWS.md` already uses the `# peskas.timor.data.pipeline X.Y.Z`
heading the script greps for.

Fix the bug while porting: the script writes `tag_name=v$version` to `$GITHUB_OUTPUT`
and then the release step uses `tag_name: v${{ steps.changelog.outputs.tag_name }}`,
producing `vv2.8.0`. The `check_tag` step compounds it with a third `v`. Emit a bare
`$version` and add the single `v` at use site.

### Pipeline workflow **[P9 ✅]**

Timor: `ubuntu-22.04`, `checkout@v4`, `build-push-action@v5`, no coasts resolution.
Standard: `ubuntu-latest`, `checkout@v5`, `build-push-action@v6`,
`FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true`, and a "Resolve latest peskas.coasts
release" step feeding `COASTS_REF`.

Timor has **11 workflows** vs Moz's 6. The seven extra
(`data-report`, `dataverse-upload`, `form-summary`, `keplergl-map`,
`upload-matched-trips`, `validation-email-sender`, plus the pipeline) are
Timor-specific; audit against the P0 secret inventory and retire the dead ones.

---

## 9. Tests — Timor is ahead, protect it **[all phases]**

| | Timor | Moz / Kenya / Zanzibar |
|---|---|---|
| `inst/tinytest/` | 4 suites, run as CI steps inside the pipeline | none |
| `tests/testthat/` | harness + 1 test | none |
| `Config/testthat/edition` | absent | `3` (Moz, unused) |

The tinytest suites gate the live pipeline — `test_validated_landings.R`,
`test_validated_pds_trips.R`, `test_merged_trips.R`, `test_public_data.R`. Update
expectations as schemas change; never delete assertions to make a phase pass. Add
`Config/testthat/edition: 3` to DESCRIPTION.

---

## 10. Timor-only files: keep / retire

| File | Verdict |
|---|---|
| `inst/tinytest/`, `tests/` | **keep** — the standard's weak spot |
| `docker-compose.yaml`, `Dockerfile` (dev), `rstudio-prefs.json` | keep |
| `inst/report/` (Rmd, bib, css, shapefiles) | keep → `R/reports.R` **[P8]** |
| `inst/export/` (README.Rmd, dataset-fields.json, PNGs) | keep, Dataverse metadata **[P8]** |
| `inst/airtable/edit-submission-link.js` | **deleted** — dead: old kobo host, hardcoded v2 asset **[P1]** |
| `inst/kepler_mapper.py`, `inst/__pycache__/` | `__pycache__` delete now; the `.py` depends on the P7 kepler decision |
| `cran-comments.md` | delete **[P11]** — never going to CRAN |
| `docs/` | gitignored but present on disk; pkgdown regenerates it **[P11]** |
| `auth/` | delete after `.env` migration **[P1]** |
| `.Rhistory` | delete, both repos have one |

## 11. Files Timor lacks

| File | Phase |
|---|---|
| `CLAUDE.md` | P0 |
| `.env`, `.env.example` | P1 |
| `inst/config_template.yml` | P1 |
| `R/peskas.timor.data.pipeline-package.R` | P1 |
| `R/api.R` | P6 |
| `.github/workflows/release.yaml` | P9 ✅ |
| `.Rproj`: `LineEndingConversion: Posix` | P1 (one line) |
