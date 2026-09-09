---
paths:
  - "R/preprocessing-surveys.R"
  - "R/survey-reshaping.R"
  - "R/ingestion.R"
---

# The survey path

One row per (submission, catch, length bin), **38 standard columns only**.
`landing_cols()` is the list and `reshape_landings()` ends with
`select(all_of(landing_cols()))`, exactly as Zanzibar's `preprocess_landings()`
does. `merge_landings()` applies it again after the bind, because the **frozen
v1 snapshot still carries the raw columns** and must not be rebuilt.

**`submitted_by` is standard and stays**: it identifies the enumerator and
`push_validation_flags()` sends it to the validation app, as Zanzibar's
`validation.R` joins it onto its flags. The 59-column raw KoBo passthrough that
used to ride alongside is gone.

## `harmonise_*()` is not `map_surveys()`

Comparing them is a category error. The WIO `map_surveys()` resolves
**labels**: it joins each frame mapping, drops the raw column at every join,
and renames the standard name into the standard slot. Timor's `harmonise_v2()`
/ `harmonise_v3()` reconcile **form versions**: they coalesce differently-spelled
KoBo questions between v2 and v3 so the two can be `bind_rows()`d into one
table. The WIO repos have no such function because they never merge two form
versions — each form keeps its own `preprocess_*()` all the way to
`merge_trips()`.

Timor's actual counterpart to `map_surveys()` is `resolve_survey_labels()`,
which does the same four joins in the same order. The divergence follows from
one merged table across form versions and is **deliberate** — do not "fix" it.

## Two shapes that look like bugs and are not

- **Length bins with no count are kept.** A catch expands to one row per 5 cm
  bin whether or not anybody was counted in it, because that is what the nested
  `length_frequency` has always held and it reaches the portal — `timor_catch`
  is 1.7 M rows of which 93% have no weight. Dropping them changes a published
  table.
- **v1 is frozen** (last submission 2020-08-28): not ingested, not
  preprocessed. `merge_landings()` reads `timor-landings-v1-frozen__*.parquet`,
  produced once per environment by a freeze script that has since been
  deleted; it is in git history at `data-raw/freeze-landings-v1.R` if the
  snapshot ever has to be rebuilt. That freeze converted v1's **fork lengths to
  total length**, so every source carries TL and `join_weights()` does not
  branch on `survey_version`. It also did v1's column reconciliation and its
  flattening to the long shape, deliberately outside the package — the form is
  dead and `preprocess_landings()` should not carry a shape nothing will
  produce again.

## Labels come from the frame

`survey_labels()` resolves `landing_site` and the GAUL columns from the
PESKAS | FRAME assets snapshot. `get_assets(conf)` returns Timor's rows already
narrowed — a six-line binding over `coasts::get_assets()`, which does the
download, the form-id filtering, the column drop and the de-duplication and
returns `taxa`, `gear`, `vessels`, `sites`, `geo`. No other country has a
narrowing helper; they filter during the download.

The snapshot is **cross-country and carries no `country` column**, so Timor's
rows are selected by the Airtable record ids of its two forms —
**resolved at run time**, not hardcoded: `get_airtable_form_id()` looks each one
up from the frame's `forms` table by the KoBo asset id already in
`ingestion.landings.{v2,v3}.asset_id`, as Kenya, Mozambique and Zanzibar do.
The lookup must hit the API because `coasts::ingest_assets()` keeps
`airtable_id` on `geo` but **drops it from `forms`** (COASTS-TODO C29). Beware:
in `forms` the column named `form_id` holds the *KoBo asset id*, while
everywhere else `form_id` holds *Airtable record ids*.

Where the frame and Timor's own reference data overlap, **Airtable is
authoritative**: taxa, gears, vessels, landing_sites, districts/regions,
pds_devices. Without it the API export would be schema-correct but full of
untranslated Tetum labels. Small vocabularies that never change are
`case_when()` lookups in R (`habitat_labels()`, `conservation_labels()`),
exactly as the other three hold theirs. **Timor reads no Google Sheet.**
