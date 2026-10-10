---
paths:
  - "R/validation.R"
  - "R/validation-functions.R"
  - "R/validate-pds-trips.R"
  - "inst/tinytest/**"
  - "tests/**"
---

# Validation

16 validators over the long table. **This is deeper than any other country
pipeline — preserve it.** The alert codes are the contract: the code →
validator table is in the header of `R/validation-functions.R` and the
descriptions in `inst/config.yml`'s `validation.alerts` block.

The KoBo validation-status client lives in coasts —
`coasts::list_validation_statuses()` / `coasts::update_validation_status()`.
Reviewers' decisions, made in the Peskas Management Platform or in KoboToolbox,
are read on every run by `read_review_decisions()` (wrapping
`coasts::review_decisions()`), so a run keeps them.
`sync_validation_status()` (the write-back) is deliberately not wired into the
pipeline.

## The validated table drops flagged submissions, as the other countries do

A submission reaches the validated long table only with no flag or a
reviewer's approval, and never once a reviewer rejected it: Kenya's,
Mozambique's and Zanzibar's rule. The validators' in-place blanking still
applies to what is kept. Before 5.4.0 Timor only blanked and kept every
submission, so the API's raw and validated tables had the same trip count.
Every downstream reader (API, Dataverse, portal) inherits the filter.

## A future landing date is flagged, not blanked

Alert 4, ~107 submissions. `landing_date` is the merge key and every time
aggregation reads it, so blanking it would hide a correctable typo instead of
surfacing it. The submission waits for review outside the validated table. The
guarantee to assert is therefore that no future date escapes **unflagged** —
not that none exists.

## `tinytest::run_test_file()` sets no exit status

It returns its results; the process exits 0 regardless. The pipeline test steps
once reported success no matter what they found.
Each step must check the result itself:

```r
r <- tinytest::run_test_file(system.file("tinytest/test_validated_landings.R",
                                         package = "peskas.timor.data.pipeline"))
if (any(!vapply(r, isTRUE, logical(1)))) quit(status = 1)
```

## The IMEI roster

Enumerators write down about 7 of a tracker's 15 digits, so `validate_imeis()`
reconstructs the full number by suffix-matching a roster of every device ever
deployed — a submission from any year can name a long-retired tracker. It
resolves 38,973 submissions.

`tracker_imeis()` unions the stored `tracker-imeis__*.parquet` archive in the
country bucket with the frame's current `MAF / WorldFish` devices, so a newly
deployed tracker needs no manual step. **The frame alone is not enough**: it
records who owns a device *now*, and 144 archived IMEIs are no longer Timor's —
26 of them with 2,722 trips in the pipeline, which would go unmatched.
