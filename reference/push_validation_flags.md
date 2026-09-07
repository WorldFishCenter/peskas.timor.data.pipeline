# Push the validation flags to the shared validation database

One `surveys_flags-<asset_id>` collection per **live** form, plus the
`enumerators_stats-<asset_id>` long form (one row per raised alert) the
cross-country validation UI reads. v1 is frozen: it has no live asset
and gets no collection.

## Usage

``` r
push_validation_flags(conf, flags)
```

## Arguments

- conf:

  The configuration file.

- flags:

  The flags frame assembled by
  [`validate_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/validate_landings.md).

## Value

Invisibly, the flags frame that was pushed.
