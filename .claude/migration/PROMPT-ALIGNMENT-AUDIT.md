# Prompt for a fresh session — alignment audit, before Phase 11

Copy everything below the line into a fresh Claude Code session started in the
**`peskas.timor.data.pipeline`** working directory.

---

Alignment audit — static assets and pipeline logic

**This is not Phase 11, and it is not a refactor.** It is the check that should
happen before the cutover: Phase 11 deletes legacy keys and merges to `main`, and
deleting the *wrong* things — or keeping divergences nobody has examined — bakes
them in. Produce an assessment and decisions. Write code only to measure.

> ## Nothing in this session touches production. Nothing.
>
> This is a read-and-measure session. The full list, because "be careful" is not
> an instruction:
>
> - **Never set `R_CONFIG_ACTIVE=production`.** Leave it unset or `default`, so
>   every bucket resolves `-dev`.
> - **Write to no production bucket**: not `timor`, `pds-timor`, `public-timor`,
>   `peskas-api-prod`, nor `peskas-coasts` (the prod hub). Reading them to
>   *measure* is fine and often the point — listing objects and downloading an
>   artefact to diff it changes nothing.
> - **Do not run `data-raw/freeze-landings-v1.R` or
>   `data-raw/convert-pds-tracks.R`.** They are Phase 11's, against production, and
>   they are the user's to run.
> - **Airtable is read-only.** `PESKAS | FRAME` is live shared data for four
>   country pipelines and `coasts::ingest_assets()` snapshots it on every run. You
>   may *propose* a table or a field; do not create, rename or populate one. There
>   is no dev copy of the frame.
> - **KoBoToolbox is read-only.** Never call `update_validation_status()` or
>   `sync_validation_status()` — they PATCH the live forms and there is no
>   development KoBo instance, so `R_CONFIG_ACTIVE` does not isolate them.
> - **MongoDB: `validation-dev` only.** Never `validation-prod`.
> - **Do not merge anything, and do not push to `main`** in any repo. Pushing the
>   Timor phase branch is safe and is the intended test mechanism — every workflow
>   sets `R_CONFIG_ACTIVE=production` only on `main`, so a branch push exercises
>   the whole pipeline against `-dev` — but do not push one just to see what
>   happens; it is a ~1h30m run.
> - **Change no code in `R/`, `inst/config.yml` or the workflows.** Findings go in
>   a document. If a measurement needs a script, put it in the scratchpad, not in
>   `data-raw/`.

## Where this session sits

The sequence the user is following:

1. **this audit session** — assess and decide, change nothing;
2. **the user walks the pipeline themselves**, using this session's output as the
   map;
3. **then Phase 11** (`.claude/migration/PROMPT-PHASE11.md`), which is where code
   is deleted and `main` is merged.

Write the deliverable for step 2. That means **ordering it by pipeline step, in
DAG order** — `ingest_metadata_tables` → `ingest_landings` → `ingest_assets` →
PDS → preprocessing → validation → merge → model → export — not alphabetically
by table and not by severity. Someone should be able to read it top to bottom
beside `data-pipeline.yaml` and know, at each step, what Timor does, what the
other three do, and whether the difference is intentional.

## Why this session exists

Walking the pipeline DAG from the top, the first step is
`ingest_metadata_tables()` — 12 Google Sheets metadata tables. **No other country
pipeline has an equivalent function.** The question raised, and it is the right
one: if the African repos are fully Airtable-driven, why is Timor still ingesting
static assets from Sheets, and is the "core functional infrastructure and
pipeline logic" actually aligned or only aligned where somebody happened to look?

The premise checks out, and more strongly than expected. Verified 2026-08-14:

- **Mozambique** declares `metadata.google_sheets.tables` (`landing_sites`,
  `catch_groups`, `vessel_type`, `habitat`, `gear_type`) and **has no code that
  reads them** — no `read_sheet`, `range_read` or `gs4_auth` anywhere in `R/`.
  Vestigial config.
- **Zanzibar** the same: five tables declared (`devices`, `sites`, `boats`,
  `catch_type`, `communities`), no reader.
- **Kenya** declares one (`BMUs`); its only Sheets code is in `R/export.R`, and it
  **writes** a sheet as an export. It does not ingest metadata either.

So Timor is the only pipeline still actively ingesting static assets from Sheets,
and the other three have already reached the target state — partly by migrating,
partly by leaving dead keys behind. That is the divergence to resolve.

## Read first

`CLAUDE.md`; `.claude/migration/PLAN.md` §2.5 (Airtable is authoritative) and
§Phase 11; `.claude/migration/STATE.md` — the **Phase 5 entry's "Deviations from
the brief"**, which is where the measured reasons for the surviving tables live,
plus the Phase 3, 4 and 7 entries and Addendum 5; and
`.claude/migration/COASTS-TODO.md` C13 and C14.

## Part 1 — the twelve tables

`PESKAS | FRAME` (`appMMEJYlJdfSJEjm`) has **17 tables**: `countries`, `regions`,
`districts`, `landing_sites`, `partners`, `forms`, `sampling_frame`, `project`,
`pds_devices`, `gears`, `vessels`, `taxa`, `enumerators`, `validation`, `frame`,
`feedback`, `Instances`.

A name-based grep of the readers, as a starting map — **treat every row as a lead
to verify, not a finding.** The grep matches literal table names and will miss
indirection through a variable or a list element.

| Sheets table | appears in `R/` | likely frame home |
|---|---|---|
| `boats` | `preprocess-metadata-tables.R` only (+3 Rmd) | `sampling_frame` / `frame`? |
| `devices` | `preprocess-metadata-tables.R`, `validation.R` | **`pds_devices`** (incomplete — see below) |
| `vms_installs` | `preprocess-metadata-tables.R` only | none; no reader since Phase 5 |
| `catch_types` | `preprocess-metadata-tables.R`, `pds-maps.R`, `preprocessing-surveys.R` | `taxa`, except `length_type` (C14) |
| `morphometric_table` | `preprocess-metadata-tables.R`, `model-taxa.R` | none — **but see the hub option** |
| `centro_pescas` | `preprocess-metadata-tables.R` only | **`landing_sites.latitude/longitude`** — new in coasts 4.7.0 |
| `stations` | `preprocess-metadata-tables.R`, `validation.R` (+2 Rmd) | `landing_sites` (label conflict) |
| `reporting_units` | **nothing matched** | `districts` (label conflict) |
| `habitat` | 7 files — the most widely read | none |
| `fishing_vessel_statistics` | `preprocess-metadata-tables.R` only | `sampling_frame` / `frame`? |
| `registered_boats` | `preprocess-metadata-tables.R`, `model-fishery.R` | `sampling_frame` / `frame`? |
| `conservation` | `preprocess-metadata-tables.R`, `export.R`, `validation.R` | none |

For each table, reach one of four dispositions, with evidence:

1. **Delete** — nothing reads the parsed output. Several look like this
   (`vms_installs`, `centro_pescas`, `fishing_vessel_statistics`, and possibly
   `reporting_units` and `boats`); confirm by tracing the parsed object, not the
   table name.
2. **Move to the frame** — the data is already there. Assert equivalence
   row-for-row before switching, the way Phase 10 did for `country` (60 taxa /
   9 gears / 2 vessels, identical both ways).
3. **Move to the frame, but the frame needs data first** — then say exactly which
   table and which fields the user must create, with row counts. This is an
   Airtable data-entry task, not a code task, and it is the honest answer for
   anything with no home. Do not invent a schema for a cross-country base
   unilaterally; propose it.
4. **Keep, with a measured reason** — legitimate, but the reason must be a
   number, and it goes in `inst/config.yml` beside the key.

Two candidates are worth flagging up front because the answer may not be Airtable
at all:

- **`morphometric_table`** — coasts 4.6.0 shipped `get_length_weight_coeffs()`,
  `get_length_length_coeffs()` and `get_taxa_morphometrics()`, and Phase 4 already
  moved Timor's weight path onto them. So the question is whether this Sheets
  table still has a *live* reader in `model-taxa.R` or is a leftover. If it is
  live, the target is the **hub**, not the frame. The acceptance gate is in
  COASTS-TODO ("Acceptance gate for any replacement") — 693 species, 5,926
  coefficient rows, mean `a` 0.02716, mean `b` 2.9781 — and total catch weight
  must not move.
- **`centro_pescas`** — Phase 10 added `latitude` / `longitude` to the frame's
  `landing_sites` (343 of 736 sites populated, all 40 Timor sites reported
  populated in earlier measurement). Check coverage for Timor specifically and
  whether anything still reads the Sheets copy.

### Blockers already measured — carry these, do not rediscover them

- **`devices`.** The frame's `pds_devices` holds 442 Timor rows against the
  Sheets' 595, and it is a strict subset. Switching moves alert 3 from **824 to
  1,475** submissions and strips the resolved `tracker_imei` — hence the matched
  trip — from **651** of them. Separately, 27 IMEIs that produce trips are in no
  frame customer at all, worth 2,791 trips and 59 landing↔trip matches. The IMEI
  list is in the Phase 7 STATE entry. **This is gated on Airtable being
  completed, by the user, not on code.**
- **`stations` / `reporting_units`.** These are the **published** labels, not
  lookups. `municipality` is Timor's reporting unit and is not `gaul_1_name`:
  only 22 of 40 sites agree, and Atauro is its own reporting unit while GAUL puts
  it inside Dili. 11 of 40 landing-site names differ between the sources.
  `format_public_data()` hardcodes five reporting-unit names and
  `get_summary_data()` hardcodes five Sheets spellings to classify the north
  coast — two of which the frame writes differently (`Tutuala` →
  `Tutuala/Valu/Savirara`, `Comando` → `Comando/Obrato/Behau`). Swapping either
  source silently reclassifies a published portal object. Any proposal here must
  pass `data-raw/compare-portal-json.R`.
- **`catch_types.length_type`** — COASTS-TODO **C14, withdrawn deliberately**.
  Field practice measures the five declaring invertebrates on total length, so
  propagating the column upstream would propagate a wrong assertion to four
  countries. Do not reopen it; the open question is only whether anything else in
  `catch_types` still has a reader.

## Part 2 — pipeline logic, not just assets

Compare Timor's thirteen-job DAG against what Mozambique, Kenya and Zanzibar run,
function by function, and classify every divergence as **deliberate and
documented**, **deliberate but undocumented**, or **unexamined drift**. Only the
third category is a finding; the second needs a line of documentation.

Already decided, with reasons on record — **do not relitigate, just verify the
reason still holds**:

- the portal contract is Timor's seven `portal-*.json` objects, not the coasts
  portal path (PLAN §2.1);
- `merge_trips()` is not `coasts::merge_survey_trips()`, which does a different
  job (COASTS-TODO C10), and no other country has a `merge_trips()` at all;
- `coasts::summarize_data()` is not called by Timor — and after Phase 10 the
  reason is no longer an upstream bug: the remaining prerequisites are local
  (seed `asfis` into `timor-dev`, declare `surveys.summaries.file_prefix`, run
  `preprocess_pds_tracks()`), and there is still no Timor consumer of the grid
  summaries. **Re-examine this one**: it is the largest single piece of the
  standard Timor does not run, and Addendum 5 changed why;
- `describe_pds_tracks()` is the one PDS product coasts has no equivalent for;
- 16 validators, the nutrients path and Dataverse publishing are Timor's, decided
  in Phase 10.

Worth a specific look, since nobody has audited them as *logic*: the ingestion
and preprocessing shapes (Timor's `harmonise_v2()`/`harmonise_v3()` versus the
WIO `map_surveys()` pattern), and whether Timor's raw-column passthrough
superset is still carried anywhere after Phase 5 moved validation onto standard
names.

## Deliverables

1. **`.claude/migration/ALIGNMENT-AUDIT.md`**, **in DAG order** — one section per
   pipeline step, and within it every static asset and every logic divergence that
   step involves, each with a disposition and the number behind it. A short
   summary table at the top (asset → disposition → blocked on what) so the detail
   is skippable on a second read.
2. **A list of Airtable data-entry tasks for the user**, with row counts and the
   exact fields, separated from anything that is code.
3. **A sequencing recommendation**: what must land before Phase 11, what can
   follow it, and what should never happen. Expect the answer to be "most of this
   follows the cutover" — a `# [legacy]` key with a live reader is not
   deletable in Phase 11 either way, so the audit mostly changes *what Phase 11
   is allowed to delete*, and that is exactly why it is worth doing first.
4. If the findings amount to real work, propose it as its own phase and say
   whether Phase 11 should run before or after it. Append a STATE entry either
   way, and update PLAN if the phase list changes.

## Rules

- **Measure, do not infer.** Every claim gets a command behind it. This migration
  has repeatedly found that the documented reason and the live behaviour differ —
  Phase 10 withdrew COASTS-TODO C17 entirely because acting on it as filed would
  have broken three pipelines.
- **Read-only against production** — the full list is at the top of this prompt.
  If a measurement seems to require a production write, it does not; say what you
  would have needed and move on.
- **Do not weaken the portal contract or delete a test assertion** to make
  something align.
- **Alignment is not the goal; a justified position is.** "Timor keeps this, and
  here is the number that says why" is a successful outcome for any given table.
  Four of the twelve may well end that way.
