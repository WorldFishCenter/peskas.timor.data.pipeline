# One row per submission

Collapses the long weighted catch table to its submission-level columns.

## Usage

``` r
validation_submissions(landings)
```

## Arguments

- landings:

  The long weighted catch table from
  [`get_weighted_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_weighted_landings.md).

## Value

A tibble with one row per submission.

## Details

`submitted_by` is set to the reporting region, not the KoBo account. One
enumerator covers each of the twelve reporting regions — Atauro has its
own, separate from the rest of Dili — but they share logins: 10 accounts
cover 97,753 submissions and one of them carries 87% of those across
every municipality, so the account name does not identify who collected
a landing. The region does, which is why this uses
`frame_reporting_region()` rather than `gaul_1_name`: that would fold
Atauro's 39,968 submissions into Dili's and merge two enumerators into
one.

Where no region resolves the account name is kept, and the column keeps
its name because the validation app reads it by that name.
