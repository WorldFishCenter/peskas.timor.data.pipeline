# Merge the pre-processed landings of every form version

Binds the long v2 and v3 tables with the frozen v1 snapshot. Column
reconciliation across form versions now happens in
[`preprocess_landings()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/preprocess_landings.md),
one version at a time, so this is a plain bind.

## Usage

``` r
merge_landings(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The (standard Apache logj4) log level used as a threshold for the
  logging infrastructure. See
  [logger::log_levels](https://daroczig.github.io/logger/reference/log_levels.html).

## Value

No output. This function is used for its side effects.

## Details

The parameters needed in `config.yml` are:

    surveys:
      landings:
        v1:
          frozen:
            file_prefix:
            version:
        v2:
          preprocessed:
            file_prefix:
            version:
        v3:
          preprocessed:
            file_prefix:
            version:
        merged:
          file_prefix:
    storage:
      google:
        key:
        options:
