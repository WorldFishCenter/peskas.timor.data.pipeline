# Prompt for the next session — Phase 11

Copy everything below the line into a fresh Claude Code session started in the
**`peskas.timor.data.pipeline`** working directory.

---

Phase 11 — cutover

This is the **last** migration phase, and the only one that changes production.
Everything before it was reversible; this one merges to `main` and lets a real
run write to `timor`, `pds-timor`, `public-timor` and — if you remove two `if:`
lines — `peskas-api-prod`.

Read in order before doing anything: `CLAUDE.md`;
**`.claude/migration/ALIGNMENT-AUDIT.md` §13 ("What Phase 11 may and may not
delete") — this is the operative list and it overrides anything below that
disagrees with it**; `.claude/migration/PLAN.md` (§Phase 11);
`.claude/migration/STATE.md` — the **alignment-audit entry at the bottom first**,
then Phase 10's, then Phase 8's "Findings that change later phases" and Phase
9's — and `.claude/migration/COASTS-TODO.md`'s status lines.

> **This prompt was written before the 2026-08-18 alignment audit and three of
> its instructions were wrong.** They are corrected in place below and flagged
> **[corrected 2026-08-18]**. Read those first.

**Send `PROMPT-PORTAL-CORRECTIONS.md` before this one.** Two published numbers
are wrong (the coast rule and the registered-boat counts) and both fixes are
decided. Landing them first lets *this* phase's dev run be asserted as **zero
portal change**, which is a much stronger gate on a deletion phase.

Execute Phase 11 only. End the session by appending a Phase 11 entry to
`.claude/migration/STATE.md`.

## Where things stand

Phases 0–10 are complete. Phase 10 upstreamed five items to `peskas.coasts`
(PR #17, merged as `989049c`) and they shipped in **release v4.7.0**
(`597003c`). Timor is **already unpinned** — `.github/workflows/data-pipeline.yaml`
resolves `repos/WorldFishCenter/peskas.coasts/releases/latest` at container build
time — so PLAN's "unpin coasts" item is **already done**; just read the resolved
ref out of the build log rather than re-doing it.

**The dev-run gate is already met.** Run
[31778254836](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/actions/runs/31778254836)
(2026-08-14, on this branch at `0573d4f`) logged `Resolved peskas.coasts ref:
v4.7.0` and went **green on all thirteen jobs**, including the four that carry
the tinytest suites. Timor's existing code works against the upstreamed hub.
What that run does *not* prove is that the hub versions are wired in — Timor's
own definitions still win over the imports — so the deletions below still need
their own run.

**coasts has since shipped v4.8.0**, so your next build resolves that, not
4.7.0. It is immaterial here, verified rather than assumed: v4.7.0..v4.8.0
touches only `summarize_data()` and `export_portal()`, and the only mentions of
either in this repo are comments in `R/api.R`, `R/pds-tracks.R`, `R/pds-maps.R`,
`inst/config.yml` and `data-pipeline.yaml`. All five Phase 10 commits are
ancestors of v4.8.0. One further commit on coasts `main`, `dedf4d6` (removes the
dead `fetch_assets()`), is **unreleased**; harmless either way, since every
`fetch_assets` reference in the country repos is a local definition, not a
`coasts::` call.

Do not read Timor's **`main`** pipeline runs as evidence about the hub: `main`
is pre-migration and has no `COASTS_REF` at all, so its green production runs
say nothing about coasts compatibility.

Branch: `feat/align-coasts-phase9`, which despite the name carries Phases 9 and
10's documentation. Decide early whether Phase 11 continues on it or branches
off it; either is fine, but **do not merge to `main` until the very end.**

## What 4.7.0 makes deletable — and what it does not

Delete only after a green dev run against 4.7.0 proves the hub versions work.

| delete from Timor | replace with | evidence it is equivalent |
|---|---|---|
| `kobo_request()`, `kobo_validation_url()`, `list_validation_statuses()`, `get_validation_status()`, `update_validation_status()` in `R/validation-functions.R` | `coasts::` equivalents | Phase 10 verified live: 22,250 rows in 22.3 s, `all.equal()` TRUE against Timor's implementation |
| the **second** upload in `ingest_assets()` (`R/ingestion.R`) — the hand-rolled hub mirror | nothing; coasts 4.7.0 writes the hub itself (C11) | verified by running it off the branch: the object landed in `peskas-coasts-dev` |
| ~~`timor_assets()`'s `metadata.airtable.form_ids` filter~~ | ~~`country == "Timor-Leste"` (C13)~~ | **[corrected 2026-08-18] DO NOT DO THIS. It silently empties two label joins.** See below |

### [corrected 2026-08-18] The `timor_assets()` swap is wrong as filed

The row above was true for the three tables it was measured on and false for the
two it was not. `timor_assets()` is applied to **five** asset tables, and
`survey_labels()` (`R/preprocessing-surveys.R:503`) applies it to `sites` and
`geo`. Measured against the live snapshot:

| table | `form_ids` rows | `country == "Timor-Leste"` rows |
|---|---|---|
| `taxa` | 60 | 60 — `identical()` TRUE |
| `gear` | 9 | 9 — `identical()` TRUE |
| `vessels` | 2 | 2 — `identical()` TRUE |
| `geo` | 37 | **0** |
| `sites` | 40 | **no such column** |

`geo.country` is a `multipleRecordLinks` field carrying Airtable record ids
(`rec7DrrSnRrlzv8BF` for Timor) — the same defect C13 found on
`landing_sites.Country` and deliberately skipped; nobody checked `geo`. Making
the swap would return zero rows for `geo` and error or empty for `sites`,
silently stripping `landing_site`, `gaul_1_*` and `gaul_2_*` from the
preprocessed table — which feeds the cross-country API export.

**Leave `timor_assets()` on `form_id` in this phase.** It works for all five
tables. Filed upstream as COASTS-TODO C24. The better long-term fix is
Mozambique's `get_airtable_form_id()` (`preprocessing-surveys.R:950`), which
resolves the record id from the KoBo asset id at run time and needs no `country`
column at all — that is Phase 12's, not this one.

**Do not delete `add_version()`** — Timor keeps it deliberately; coasts exports a
body-identical copy and delegating would mean editing ~40 call sites plus three
`inst/report/` drivers for no behavioural gain. The reason is in CLAUDE.md.

### [corrected 2026-08-18] Three things this phase must NOT delete

- **The 59-column raw KoBo passthrough** carried through `merge_landings()` and
  `calculate_weights()`. No file in `R/` reads one, but
  `inst/report/enumerators_summary.Rmd` reads **nine** (`_id`,
  `landing_site_name`, `Ita_koleta_dadus_husi_atividad`, `no_boats`,
  `reason_no_activity`, `date`, `today`, `start`, `end`) and
  `generate_enumerators_report.R` is the **last step of the active
  `export-trips` job**. `CLAUDE.md` and `AUDIT.md` both imply this phase removes
  it. They are wrong.
- **`all_trips__*.rds` stays `.rds`.** The recorded reason ("no cross-country
  counterpart") is false — Mozambique's `merge_trips()` is line-for-line Timor's
  and writes parquet — but the real reason holds: three readers against a live
  portal.
- **Seven of the twelve Sheets metadata tables have live readers**: `devices`,
  `stations`, `reporting_units`, `catch_types`, `morphometric_table`, `habitat`,
  `conservation`. `catch_types.interagency_code` is also a live tinytest
  assertion (`test_validated_landings.R:70`).

Timor's local copies winning over the hub's is **not** a bug while both exist —
a package's own definitions take precedence over an import — so a green run
before deletion proves 4.7.0 does not break Timor, not that the hub versions are
wired in. Those are two separate tests; do both.

## The rest of the phase

- **Legacy config keys.** `inst/config.yml` is a superset; every legacy key is
  marked `# [legacy]`. Delete a key only after grepping for its last reader.
  `validation.google_sheets` and `validation.version.preprocess` lost theirs in
  Phase 5; `VALID_SHEET_ID` has had none since Phase 5.
- **Dead code. [corrected 2026-08-18 — the list was incomplete.]** Re-measured
  by call-site count across `R/`, `inst/tinytest/`, `inst/report/`,
  `.github/workflows/` and `data-raw/`, excluding definitions and comments.
  Over **1,000 unreachable lines**, in two blocks plus a scatter:

  - **`R/model-fishery.R` lines 646–1289 — 644 lines, the whole second half of
    the file.** A complete second glmmTMB implementation rooted at
    `model_indicators()` (0 callers). Everything it reaches — `run_models()`,
    `model_landings()`, `model_catch()`, `model_catch_per_taxa()`,
    `model_value()`, `estimate_statistics()`, `estimates_per_taxa()` — is called
    only from inside that dead subgraph. Deleting the root without the family
    leaves 600 lines of orphans; decide on the whole block at once.
  - **`R/pds-maps.R` — 429 of 636 lines.** Two dead chains: `ingest_pds_map()`
    (81–372) → `get_sync_tracks()` → `get_full_trips()` / `ingest_complete_tracks()`,
    and `ingest_kepler_tracks()` (485–557) → `kepler_mapper()` (558–589), plus
    `inst/kepler_mapper.py`. **Only `convert_taxa_names()` (373–484) and
    `get_timor_boundaries()` (590–636) are live** — both called by
    `format-public-data.R`. Do not delete the file.
  - **Scattered, all 0 callers:** `ingest_pds_matched_trips()`
    (`merge-trips.R:85`), `get_validation_flags()` (`get-cloud-files.R:530`),
    `get_tracks_map()` (`get-cloud-files.R:473`), `pt_validate_flags()`
    (`preprocess-metadata-tables.R:174`), `send_sites_report()` (`reports.R:28`),
    `delete_dataverse()` / `publish_dataverse()` (`export-dataverse.R`), and
    `get_preprocessed_metadata()` (`get-cloud-files.R:293`) — the last being
    **body-identical** to `get_preprocessed_sheets()`, with its only caller
    `inst/report/unanswered_summary.Rmd`, whose driver
    `inst/report/generate_form_summary.R` lost its workflow in Phase 9. Delete
    both files with it.

  **Keep `sync_validation_status()`**: unwired on purpose, not dead. Its only
  match in the workflow is a comment saying so.
- **User decision 2026-08-18 — drop the boats section of
  `inst/report/data_report.Rmd`**, and with it the `vessels_stats` summary at its
  line 48, the `boats` and `fishing_vessel_statistics` config entries, and
  `pt_validate_boats()` / `pt_validate_vessels_stats()`. Those two tables had no
  other reader.
- **Also deletable, measured:** `vms_installs` and `centro_pescas` (zero readers
  anywhere; `pt_validate_vms_installs()` and `pt_validate_centro_pescas()` go
  with them), the `centro_pescas` comment at `inst/config.yml:310` (it claims the
  table is "the only source of landing-site lat/lon"; the table has no lat/lon
  columns at all and the frame has coordinates for all 40 Timor sites), and the
  `aL`/`bL` coercion in `pt_validate_morphometric_table()` (both columns are
  100 % `NA`).
- **Directories**: `auth/`, `docs/`, `inst/__pycache__`, `cran-comments.md`, and
  the untracked `peskas.mozambique.data.pipeline/` reference copy.
- **Cloud cleanup**, all deferred here: the 103,373 dead
  `pds-track-<id>__*__.csv.gz` in `pds-timor-dev`, and the 45 leaked
  absolute-path objects in `public-timor` dating from January 2026.
- **Re-enable the three `disabled_inactivity` workflows** —
  `data-report.yaml`, `dataverse-upload.yaml`, `validation-email-sender.yaml` —
  **only after** the merge. A cron fires from the default branch, so enabling
  them earlier runs `main`'s pre-migration code against production.
- **The gate**: a full green run on the phase branch (dev buckets), all four
  tinytest suites, then `Rscript data-raw/compare-portal-json.R` against the
  Phase 0 golden. Then merge, then watch one production run end to end.

## Load-bearing points

- **`data-raw/convert-pds-tracks.R` against production is mandatory before the
  merge.** `pds-timor` (prod) holds **zero** `pds-tracks_*.parquet`; without the
  conversion the first production run re-fetches ~98k tracks from the PDS API.
  Phase 10's C18 guard does **not** catch this: after the prefix filter the
  listing is empty, which takes the early `character(0)` return and is
  indistinguishable from a fresh bucket. `pds-timor-dev` is fully converted
  (101,960 objects, measured 2026-08-13), so dev runs are cheap.
- **`data-raw/freeze-landings-v1.R` against production is also mandatory** —
  `merge_landings()` reads a frozen v1 snapshot that exists in `timor-dev` only
  (re-verified 2026-08-18: `gs://timor` holds **0**). **[corrected 2026-08-18 —
  run it at the END of this phase, not before it.]** It produces a *permanent*
  artefact and is built by package code — `get_preprocessed_sheets()`,
  `get_taxa_list()`, and four internals via `:::` (`sum_fishers()`,
  `mesh_size_mm()`, `resolve_catch_taxa()`, `resolve_survey_labels()`). Running
  it before this phase's deletions would commit a permanent production object
  built by code that is about to change. Order: deletions → green dev run →
  freeze → merge.
  `convert-pds-tracks.R` has the opposite property and can run at any time,
  including in parallel with this session: it is purely additive, idempotent
  (`filter(!target %in% already)`) and depends only on `read_config()` and
  `conf$pds$pds_tracks$file_prefix`, which is `[new]`, not `[legacy]`.
- **Timor's first `peskas-api-prod` write is a separate decision.** It is the two
  `if: ${{ !endsWith(github.ref, '/main') }}` lines on the API export steps.
  Deleting them is not implied by "merge to main".
- **`log_threshold = logger::INFO` at the `coasts::` call sites.** C21 is fixed
  upstream and the fix is in 4.7.0, so removing the argument is now *safe* — but
  it costs nothing and protects against a future regression. PLAN left the call
  to this phase; make it deliberately either way.
- **The portal contract is seven objects** and `portal-*.json` numbers are
  host-sensitive at the fourth decimal — read a numeric diff with tolerance and
  let the structural assertions carry the weight.
- Harness traps, all cost time before: `devtools::load_all()` reaches neither
  `coasts::read_config(package = )` nor `furrr`/`future` workers — use
  `devtools::install()` + `library()`; the tinytest suites need
  `dotenv::load_dot_env('<repo>/.env')` in the same `Rscript` call; and
  `add_version()` only stamps a git sha when the working directory is inside the
  repo.
- **`R CMD check` does not catch a call that omits a required argument** —
  `codetools` does not report it. Phase 10 broke a caller this way and only
  runtime surfaced it. When you change a signature, grep every call site.
- **Two lessons from Phase 10's fallout in the country repos**, both of which
  cost live pipeline runs and both avoidable:
  1. **Never write an elision inside code someone may paste.** A recommended
     snippet contained `str_detect(.data$form_id, ...)`, where `...` stood for
     "the existing pattern". It was pasted literally into Mozambique and
     Zanzibar; inside a `purrr` `~` lambda `...` is the lambda's own arguments,
     so `str_detect()` received the tibble as its pattern and **every
     preprocessing run aborted** until it was fixed. Write the whole line or
     name a variable.
  2. **`select(-c("x"))` errors when `x` is absent; `-any_of()` does not.**
     Stripping `country` at read time made `map_surveys()`' existing
     `select(-c("form_id", "district_code", "country"))` fail in both repos.
     When you remove a column upstream, grep for every negative select that
     names it.

## Things Phase 10 learned about the hub repo, if you go back into it

- `peskas.coasts/.github/workflows/data-pipeline.yaml` is `on: push:` with **no
  branch filter**, so pushing *any* branch there runs the whole coasts pipeline;
  on `main` it runs against **production**.
- On 2026-08-13 a merge to coasts `main` produced **no workflow runs at all** —
  GitHub created one check suite and concluded it `skipped`, with Actions
  enabled, no `[skip ci]` token and no filter excluding it. Cause unexplained.
  If a coasts release ever fails to appear after merging a NEWS bump,
  `gh release create` by hand is equivalent: every country resolves the version
  via `repos/.../releases/latest`.
- The assets snapshot is **data**, read at `version = "latest"`, so a schema
  change there reaches Mozambique, Kenya and Zanzibar on their next cron
  regardless of any release. All three carry a read-time
  `-any_of(c("country","latitude","longitude"))` drop as of 2026-08-13.

## Carried user actions

Rotate the credentials exposed in past CI logs (four repos' run history);
rotate `ANTHROPIC_API_KEY`; delete the obsolete `AIRTABLE_KEY` **and
`VALID_SHEET_ID`** GitHub secrets; and re-push `AIRTABLE_TOKEN` if its value
changed locally.

**[corrected 2026-08-18] The Airtable device gap is two jobs, not one.** Adding
the 27 IMEIs to a frame customer is worth 2,791 trips and 59 landing↔trip
matches on the *PDS* side — but it recovers **zero** survey-side matches.
Measured: matching submissions against **all 910** `pds_devices` rows, ignoring
the customer filter entirely, gives results identical to the 457-row Timor
subset. The survey-side gap is **144** Sheets IMEIs absent from `pds_devices`
altogether, and that is what gates moving `validate_imeis()` off the Sheets.
