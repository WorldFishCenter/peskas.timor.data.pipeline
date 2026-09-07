# Write validation statuses back to KoBoToolbox

Sets each submission's KoBoToolbox validation status from the flags
already pushed to the shared validation database, so the enumerators see
the review queue in the form itself. Only submissions whose status
differs are patched.

## Usage

``` r
sync_validation_status(versions = c("v2", "v3"), log_threshold = logger::DEBUG)
```

## Arguments

- versions:

  Form versions to synchronise. v1 is frozen and has no collection.

- log_threshold:

  The (standard Apache logj4) log level used as a threshold for the
  logging infrastructure.

## Value

Invisibly, a tibble of per-submission update results.

## Details

**Not part of the recurring pipeline, on purpose.** There is no
development KoBoToolbox instance, so this mutates the two live forms
whatever `R_CONFIG_ACTIVE` says — running it from a migration branch
would change production review state. Wire it into a workflow once that
is a deliberate decision rather than a side effect.
