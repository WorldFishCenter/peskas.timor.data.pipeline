# Prompt for the next session — Phase 11a: cut and align

Copy everything below the line into a fresh Claude Code session started in the
**`peskas.timor.data.pipeline`** working directory.

**This is the "delete the stale code and prove Timor runs like the other three"
session, and it touches production nowhere.** The cutover — the freeze, the
merge, the production run — is `PROMPT-PHASE11B.md`, a separate prompt you send
afterwards. Do not send them together.

Prerequisite: both commits of `PROMPT-PORTAL-CORRECTIONS.md` are in and green.

---

Phase 11a — cut the stale code, delegate to the hub, prove it green on dev

> ## Nothing in this session touches production.
>
> - **Never set `R_CONFIG_ACTIVE=production`.** Leave it unset or `default`.
> - **Write to no production bucket**: not `timor`, `pds-timor`, `public-timor`,
>   `peskas-api-prod`, nor `peskas-coasts`. Reading them to measure is fine.
> - **Do not merge to `main`, and do not run `data-raw/freeze-landings-v1.R`.**
>   The freeze produces a *permanent* artefact built by the code you are about to
>   change; it belongs at the start of 11b, after these deletions are green.
> - `data-raw/convert-pds-tracks.R` against production is the **user's** to
>   launch and is independent of this session.
>
> Pushing this branch is the test mechanism and is safe: every workflow sets
> `R_CONFIG_ACTIVE=production` only on `main`, so a branch push exercises all
> thirteen jobs against the `-dev` buckets. A run is ~35 minutes.

Read in order: `CLAUDE.md`; **`.claude/migration/ALIGNMENT-AUDIT.md` §13 ("What
Phase 11 may and may not delete") — the operative list, it overrides anything
below that disagrees**; `.claude/migration/STATE.md` — the alignment-audit entry
at the bottom first, then Phase 10's; and `.claude/migration/COASTS-TODO.md`'s
status lines.

End the session by appending a Phase 11a entry to `.claude/migration/STATE.md`.

## Where things stand

Phases 0–10 are complete. Phase 10's five items were merged into `peskas.coasts`
(PR #17) and shipped in **v4.7.0**; coasts has since tagged **v4.8.0**, which the
next container build resolves. v4.7.0..v4.8.0 touches only `summarize_data()` and
`export_portal()`, neither of which Timor calls — every mention in this repo is a
comment. Timor is already unpinned: the workflow resolves
`repos/WorldFishCenter/peskas.coasts/releases/latest` at build time.

Branch: `feat/align-coasts-phase9`.

**A green run before deletion proves 4.7.0 does not break Timor. It does not
prove the hub versions are wired in** — a package's own definitions win over its
imports, so Timor's local copies are still what execute. Those are two separate
tests. The deletions below are what turn the second one on, and they are the
substance of this session.

## 1. The alignment deletions — this is the point of the session

| delete from Timor | replaced by | evidence |
|---|---|---|
| `kobo_request()`, `kobo_validation_url()`, `list_validation_statuses()`, `get_validation_status()`, `update_validation_status()` in `R/validation-functions.R` | the `coasts::` equivalents | Phase 10 verified live: 22,250 rows in 22.3 s, `all.equal()` TRUE against Timor's implementation |
| the **second** upload in `ingest_assets()` (`R/ingestion.R`) — the hand-rolled hub mirror | nothing; coasts 4.7.0 writes the hub itself (C11) | verified off this branch: the object landed in `peskas-coasts-dev` |

**Do not delete `add_version()`.** Timor keeps it deliberately — coasts exports a
body-identical copy and delegating would mean editing ~40 call sites plus three
`inst/report/` drivers for no behavioural gain. Reason is in `CLAUDE.md`.

**Do not swap `timor_assets()` onto `country`.** The earlier prompt listed this
as a verified-equivalent change. It is not, and it fails silently. Measured
against the live snapshot:

| table | `form_ids` rows | `country == "Timor-Leste"` rows |
|---|---|---|
| `taxa` / `gear` / `vessels` | 60 / 9 / 2 | 60 / 9 / 2 — `identical()` TRUE |
| `geo` | 37 | **0** |
| `sites` | 40 | **no such column** |

`survey_labels()` (`R/preprocessing-surveys.R:503`) applies `timor_assets()` to
`sites` and `geo`. `geo.country` is a `multipleRecordLinks` field holding Airtable
record ids (`rec7DrrSnRrlzv8BF` for Timor) — the defect C13 found on
`landing_sites.Country` and skipped; nobody checked `geo`. The swap would strip
`landing_site`, `gaul_1_*` and `gaul_2_*` from the preprocessed table, which
feeds the cross-country API export, with no error. **Leave it on `form_id`.**
Filed as COASTS-TODO C24; the real fix is Mozambique's `get_airtable_form_id()`,
and that is Phase 12's.

## 2. Three things this session must NOT delete

- **The 59-column raw KoBo passthrough** carried through `merge_landings()` and
  `calculate_weights()`. Nothing in `R/` reads one, but
  `inst/report/enumerators_summary.Rmd` reads **nine** — `_id`,
  `landing_site_name`, `Ita_koleta_dadus_husi_atividad`, `no_boats`,
  `reason_no_activity`, `date`, `today`, `start`, `end` — and
  `generate_enumerators_report.R` is the last step of the **active**
  `export-trips` job. `CLAUDE.md` and `AUDIT.md` both imply this phase removes
  it. They are wrong.
- **`all_trips__*.rds` stays `.rds`.** The recorded reason ("no cross-country
  counterpart") is false — Mozambique's `merge_trips()` is line-for-line Timor's
  and writes parquet — but the real reason holds: three readers against a live
  portal.
- **Seven of the twelve Sheets metadata tables have live readers**: `devices`,
  `stations`, `reporting_units`, `catch_types`, `morphometric_table`, `habitat`,
  `conservation`. `catch_types.interagency_code` is also a live tinytest
  assertion (`inst/tinytest/test_validated_landings.R:70`).

## 3. Dead code — over 1,000 unreachable lines

Re-measured 2026-08-18 by call-site count across `R/`, `inst/tinytest/`,
`inst/report/`, `.github/workflows/` and `data-raw/`, excluding definitions and
comments.

- **`R/model-fishery.R` lines 646–1289 — 644 lines, the entire second half.** A
  complete second glmmTMB implementation rooted at `model_indicators()` (0
  callers). Everything it reaches — `run_models()`, `model_landings()`,
  `model_catch()`, `model_catch_per_taxa()`, `model_value()`,
  `estimate_statistics()`, `estimates_per_taxa()` — is called only from inside
  that dead subgraph. Decide on the whole block at once; deleting the root alone
  leaves 600 lines of orphans.
- **`R/pds-maps.R` — 429 of 636 lines.** Two dead chains: `ingest_pds_map()`
  (81–372) → `get_sync_tracks()` → `get_full_trips()` / `ingest_complete_tracks()`,
  and `ingest_kepler_tracks()` (485–557) → `kepler_mapper()` (558–589), plus
  `inst/kepler_mapper.py`. **`convert_taxa_names()` (373–484) and
  `get_timor_boundaries()` (590–636) are live** — `format-public-data.R` calls
  both. Do not delete the file.
- **Scattered, all 0 callers:** `ingest_pds_matched_trips()` (`merge-trips.R:85`),
  `get_validation_flags()` (`get-cloud-files.R:530`), `get_tracks_map()`
  (`get-cloud-files.R:473`), `pt_validate_flags()`
  (`preprocess-metadata-tables.R:174`), `send_sites_report()` (`reports.R:28`),
  `delete_dataverse()` / `publish_dataverse()` (`export-dataverse.R`), and
  `get_preprocessed_metadata()` (`get-cloud-files.R:293`) — the last
  **body-identical** to `get_preprocessed_sheets()`, its only caller
  `inst/report/unanswered_summary.Rmd`, whose driver
  `inst/report/generate_form_summary.R` lost its workflow in Phase 9. Delete both
  files with it.

**Keep `sync_validation_status()`** — unwired on purpose, not dead. Its only
match in the workflow is a comment saying so.

Deleting exported names means regenerating `NAMESPACE` (`devtools::document()`).

## 4. Sheets metadata tables — four go

- **`vms_installs`** and **`centro_pescas`**: zero readers anywhere.
  `pt_validate_vms_installs()` and `pt_validate_centro_pescas()` go with them.
  Also delete the comment at `inst/config.yml:310` claiming `centro_pescas` is
  "the only source of landing-site lat/lon" — the table has no lat/lon columns at
  all, and the frame carries coordinates for all 40 Timor sites.
- **`boats`** and **`fishing_vessel_statistics`**: user decision 2026-08-18 —
  drop the boats section of `inst/report/data_report.Rmd` and the `vessels_stats`
  summary at its line 48, then both config entries and
  `pt_validate_boats()` / `pt_validate_vessels_stats()`.
- **`registered_boats`**: its config entry goes only if the portal-corrections
  commit that moved `estimate_fishery_indicators()` onto the frame is already
  green. Check before deleting.
- Also drop the `aL`/`bL` coercion in `pt_validate_morphometric_table()` — both
  columns are 100 % `NA`.

## 5. The rest

- **Legacy config keys.** `inst/config.yml` is a superset; every legacy key is
  marked `# [legacy]`. Delete one only after grepping for its last reader.
  `validation.google_sheets` and `validation.version.preprocess` lost theirs in
  Phase 5. Note `pds.pds_trips` / `pds.pds_tracks` are `[new]` and stay;
  `pds.trips` / `pds.tracks` are the legacy pair.
- **Directories**: `auth/`, `docs/`, `inst/__pycache__`, `cran-comments.md`, and
  the untracked `peskas.mozambique.data.pipeline/` reference copy.
- **Cloud cleanup, `-dev` only in this session**: the dead
  `pds-track-<id>__*__.csv.gz` objects in **`pds-timor-dev`**. The 45 leaked
  absolute-path objects live in **`public-timor`**, which is production — they
  are 11b's.
- **`log_threshold = logger::INFO` at the `coasts::` call sites.** C21 is fixed
  upstream and in 4.7.0, so removing the argument is now safe — but it costs
  nothing and guards against a regression. Make the call deliberately either way.

## The gate

1. A full green run on the branch — all thirteen jobs, all four tinytest suites.
2. `Rscript data-raw/compare-portal-json.R` against the Phase 0 golden.
3. **Assert zero portal change.** That is why the two corrections landed first:
   this phase deletes code and must move no published number. If a portal value
   moves, a deletion was not inert and that is the finding.

Portal numbers are host-sensitive at the fourth decimal — read numeric diffs with
tolerance and let the structural assertions carry the weight.

## Harness traps, all of which have cost time before

- `devtools::load_all()` reaches neither `coasts::read_config(package = )` nor
  `furrr`/`future` workers — use `devtools::install()` + `library()`.
- The tinytest suites need `dotenv::load_dot_env('<repo>/.env')` in the same
  `Rscript` call.
- `add_version()` only stamps a git sha when the working directory is inside the
  repo.
- **`R CMD check` does not catch a call that omits a required argument** —
  `codetools` does not report it. Phase 10 broke a caller this way and only
  runtime surfaced it. When you change a signature, grep every call site.
- **Never write an elision inside code someone may paste.** A Phase 10 snippet
  contained `str_detect(.data$form_id, ...)` where `...` meant "the existing
  pattern". Pasted literally into Mozambique and Zanzibar, `...` became the
  lambda's own arguments and **every preprocessing run aborted**. Write the whole
  line or name a variable.
- **`select(-c("x"))` errors when `x` is absent; `-any_of()` does not.** When you
  remove a column upstream, grep every negative select that names it.

## Rules

- **Do not weaken or delete a test assertion** to make something pass. Tests are
  Timor's advantage over the other three pipelines.
- **Do not touch production.** The full list is at the top.
- Do not start 11b in this session.
