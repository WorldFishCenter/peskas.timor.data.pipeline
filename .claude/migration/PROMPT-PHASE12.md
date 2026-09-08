Phase 12 — static assets and the standard's downstream half

Read in order: `CLAUDE.md`; `.claude/migration/STATE.md` — the "Current
position" block and the **Phase 11b cutover entry at the bottom**;
`.claude/migration/ALIGNMENT-AUDIT.md` §10, §11 and §15;
`.claude/migration/COASTS-TODO.md` C23 and C26.

Work on a new branch off `main` (`0c75ee9`). End the session by appending a
STATE.md entry. **One phase per session.**

## Where the repo is

The migration is **merged and live** — PR #126, `0c75ee9`, release v4.0.0,
production run 34091315009 green 13/13, portal republished 2026-09-07. Phases
0-11b are complete. Production carries the migrated numbers: catch -18.4%,
price/kg +17.8%, nutrient supply -26.0% against the previous live set.

**coasts is at 4.10.0 and that is a hard floor.**

## Correct §15 before you use it

`ALIGNMENT-AUDIT.md` §15's "Proposed Phase 12" list is stale on its first two
items — its own ordering block above it says they went *before* Phase 11, and
they did. Verified against the code 2026-09-07:

1. **`registered_boats` -> `geo.total_boats` is DONE.** `get_registered_boats()`
   ([get-cloud-files.R](../../R/get-cloud-files.R)) reads
   `timor_assets(get_assets(conf)$geo, conf)`. Shipped in `5a83f11`. Do not
   redo it.
2. **The North Coast definition is HALF done** — see item A below.

So Phase 12 is items **A-E** below, not §15's 1-7.

## A. One site -> coast table, read by both call sites

**Still live, and it is two different definitions inside one published object.**
`summary_data` carries both:

| call site | rule | feeds |
|---|---|---|
| [export.R:337-350](../../R/export.R#L337) | municipality list, **includes Manatuto and Lautem** | `summary_data$estimated_revenue` |
| [format-public-data.R:717-733](../../R/format-public-data.R#L717) | 5 municipalities (**no Manatuto, no Lautem**) **or** 5 hardcoded landing-site names | `summary_data$n_surveys` |

`602a110` added `"Lautem"` to the first list, which is why its own comment now
reads *"the two rules agree on all but 3 of 76k landings (Lore 2, Welaluhu 1)"*
— and that comment explicitly defers the durable fix to Phase 12: **one
site->coast table read by both**.

Coast is a property of the **landing site**, not the municipality. `export.R`
approximates at municipality level only because the model has already collapsed
the data by then and the site is gone. The fix is a config-level
`site -> coast` list, or a column on the frame's `sites` table, resolved before
that collapse.

While you are in there: [format-public-data.R:729-731](../../R/format-public-data.R#L729)
writes `is.na(.data$municipality)` **three times** in one `case_when` branch.
Harmless, equivalent to one test, but it is noise in a rule that is otherwise
load-bearing.

**Decision for the user:** where the table lives (config vs frame), and whether
Manatuto is North. **Gate:** `data-raw/compare-portal-json.R`, and state the
`summary_data` delta in advance.

## B. `timor_assets()` off the hardcoded record ids

`timor_assets()` filters on `metadata.airtable.form_ids`, two literal Airtable
record ids (`recY5MD03ZDwJUBB3`, `rechg17V73uqnVu2T`). §15 proposed adopting
Mozambique's `get_airtable_form_id()`, which resolves them from the KoBo asset
id at run time.

**The landscape changed — coasts 4.9.0 shipped `coasts::get_assets()`**, the
read-side counterpart to `ingest_assets()`, which does the download, the
form-id filtering and the column drop in one call. Timor has its own
`get_assets()` + `timor_assets()` pair. Three things to check before
delegating, because a naive swap loses data:

- `coasts::get_assets(drop_cols = c("country", "latitude", "longitude"))` by
  default. **Timor's 40 frame sites all carry lat/lon.** Check who reads them.
- coasts returns **5** tables (`taxa`, `gear`, `vessels`, `sites`, `geo`);
  Timor's returns **8** (plus `forms`, `devices`, `frame`). Establish which of
  the extra three have live readers before dropping them.
- `coasts::form_id_pattern()` only builds the regex. It does **not** resolve
  ids from an asset id, so the "off hardcoded ids" half still needs Moz's
  `get_airtable_form_id()`.

**This item is behaviour-neutral or it is wrong.** **Gate:** assert the row
counts before and after — 60 taxa, 9 gear, 2 vessels, 40 sites, 37 geo — and
that `survey_labels()` still emits `landing_site`, `gaul_1_*` and `gaul_2_*`.
COASTS-TODO C24 records that the obvious `country`-column swap fails silently
here and strips exactly those columns, which feed the cross-country API export.

## C. `devices` -> `pds_devices` — blocked on Airtable data

**Do not start this until the user has added the missing IMEIs.** 144 Sheets
IMEIs are absent from `pds_devices` in PESKAS | FRAME. Switching first takes
alert 3 from **824 to 1,475** submissions and strips the resolved
`tracker_imei` — hence the matched trip — from **651** of them.

(The 27 IMEIs in the Phase 7 entry are a *different* gap, on the PDS side,
worth 2,791 trips and 59 landing-trip matches. Both are real; neither fixes the
other.)

**Gate:** alert 3 back to 824, and 0 lost `tracker_imei`.

## D. `stations` / `reporting_units` — largest, and last

§10 measured this and it is smaller than the earlier record claimed. Three
separable pieces:

- **`stations`**: 40/40 codes match the frame, zero orphans either way, but
  **11 of 40 names differ** — and they are real differences, not spelling
  variants (e.g. code 8 Sheets `Tutuala` vs frame
  `Tutuala/Valu/Savirara`). These are the **published** `landing_site` labels.
- **`reporting_units`**: 22 of 40 agree. The 18 that do not are three different
  things — **8** are the Atauro semantic difference (Timor reports Atauro as its
  own unit, GAUL nests it in Dili; the frame supports the Timor view via
  `gaul_2_name`), **9** are diacritics only (Lautem/Lautém, Liquica/Liquiçá,
  Oecusse/Oecussi), and **1** is a real conflict: code 33 **Welaluhu**, Sheets
  says `Manatuto`, the frame says `Manufahi / Fatuberliu`.
- **Fifteen hardcoded literals across three files** depend on the Sheets
  vocabulary. [format-public-data.R:785](../../R/format-public-data.R#L785)
  alone has 5 municipality plus 5 landing-site names, and **two of those five
  sites (`Tutuala`, `Comando`) are among the 11 the frame writes differently** —
  so switching the source silently drops them.

**Decisions for the user:** the accent-normalisation rule, the Atauro rule, and
Welaluhu (an Airtable data question, not code). **Gate:**
`data-raw/compare-portal-json.R`, plus the API `landing_site` column reviewed
against Kenya, Mozambique and Zanzibar — §8 L5 records that Timor is the only
country publishing a non-frame `landing_site`.

## E. Documentation only

- **`coasts::generate_fleet_analysis()` has never been examined** (§12 L4).
  Establish whether Timor should call it, then write down the answer.
- **The `harmonise_v2()`/`harmonise_v3()` vs `map_surveys()` distinction**
  (§4 L8). Two shapes doing related jobs; nobody has written down why.

## Explicitly out of scope

- `coasts::summarize_data()` and the H3 grid summaries — still no Timor
  consumer, still blocked on COASTS-TODO C17. C20 is the second reason.
- **`morphometric_table`'s move to the hub (C23).** Needs the upstream table
  first, and it carries an unresolved numbers question — see "Carried" below.
- Anything that renames or drops one of the seven portal objects, or changes a
  key inside them.

## Before Phase 12, and not part of it

**Manually dispatch the three re-enabled workflows.** They reactivated
themselves when the merge pushed to `main` — GitHub does that to
`disabled_inactivity` workflows — and **none has ever run on migrated code**.
Two send things outward. `data-report.yaml` is first: it is the **only real test
of the `data_report.Rmd` gear-label fix**, which is verified against production
labels but has never been rendered end to end.

## Carried, and each needs a decision rather than code

- **COASTS-TODO C26's remaining half.** The additive-alias shape
  (`taxa_search_aliases()`) belongs in coasts so every country gets it; 4.10.0
  fixed only the silent-drop warning. Adopting it in Mozambique and Zanzibar
  **renames published taxa there**, so it is their re-baselining, not Timor's.
- **COASTS-TODO C23**, and it is not a file move. Two open questions: is the
  curated table **global or per-country** (global means one country's curation
  moves another's published catch), and what happens to the **axis residual** —
  **362 of 559** curated rows are fitted on CW/CL/ML/ShL and applied to a total
  length, ~**3.5%** of national catch, and only **12 of 98** curated species have
  any FishBase conversion, so it cannot be fixed the way the FishBase half was.
  Relocating the CSV without deciding this carries the residual along unfixed.
- **`MZZ` publishes 4,583 kt of modelled catch with zero observed weight.**
  `Actinopterygii` is a class FishBase files as `Teleostei`, so nothing resolves.
  It is one of the 13 `models.modelled_taxa` and is exempt in
  `assert_taxa_coverage()`. Either give it a pool or drop it from
  `modelled_taxa` — the present state is neither.
- **`data-raw/delete-leaked-portal-objects.R` mis-reports.** It printed
  `0 of 45` while deleting all 45: googleAuthR retries the DELETE, the retry
  404s because the object is already gone, and the `tryCatch` logs a failure.
  Treat 404 as success before reusing that shape.
- **Credential rotation across four repos**, and the obsolete `AIRTABLE_KEY` /
  `VALID_SHEET_ID` GitHub secrets. User actions, still open.

## Verification, for every item that moves a number

- **Run today's code and the candidate in the same session, against the same
  FishBase snapshot, and diff those.** A single run's total is not the effect of
  a change. The taxa-path session learned this the hard way: two runs a day
  apart on identical code differed by 4% in national catch because the FishBase
  release moved underneath, and "varies between runs" and "varies between
  environments" are indistinguishable from the artefacts alone.
- `data-raw/compare-portal-json.R` against a snapshot taken **before** you
  start. 0 structural failures is the pass; numeric movement needs a human.
- All four tinytest suites green. **Never weaken an assertion.**
- `devtools::check()` — the baseline is 0 errors, 0 warnings, 3 NOTEs;
  `testthat` 40 pass.
- State the delta per taxon or per region **before** the change, and get the
  user's sign-off on the number, not after.

## Rules

- **Never write to a production bucket.** `.Renviron` pins
  `R_CONFIG_ACTIVE=default` and R applies it *after* the command line, so a bare
  `R_CONFIG_ACTIVE=production Rscript …` silently targets `-dev`. For a
  deliberate production **read**, use `R_ENVIRON_USER=/dev/null` or `use_prod()`.
- **A push to any non-main branch runs the whole pipeline against the `-dev`
  buckets.** That is the integration test; use it.
- **Pass `conf` to `coasts::get_taxa_morphometrics()`.** Without it coasts
  resolves `"latest"` from its own configuration and the
  `metadata.fishbase.db_version: "25.04"` pin silently does nothing.
- `devtools::load_all()` reaches neither `coasts::read_config(package = )` nor
  `furrr` workers — use `devtools::install()` + `library()`.
- The tinytest suites need `dotenv::load_dot_env('<repo>/.env')` in the same
  `Rscript` call.
- Run scripts from the **repo root** — `add_version()` only stamps a git sha
  when the working directory is inside the repo.
- **`main` may be behind `origin/main`.** The PR was merged on GitHub, so a
  local `main` can sit at the pre-migration tip `90ede9a`, where `CLAUDE.md`
  and `STATE.md` do not exist — it presents as "someone deleted the docs", and
  it stages the gitignored `reference/` snapshot. `git merge --ff-only
  origin/main`. Never commit from that state.
- **Do not commit, push or merge.** Hand the work back to the user.
