# Export Timor's district-months to the cross-country API bucket

Writes the API's district-months (`timor/aggregated/district-months`)
with one row per reporting region and month, in place of the per-post
rows
[`coasts::export_portal()`](https://rdrr.io/pkg/coasts/man/export_portal.html)
writes just before it: Timor counts its boats, and this pipeline models
its catch, per municipality. The survey figures are the posts'
([`coasts::summarize_data()`](https://rdrr.io/pkg/coasts/man/summarize_data.html))
put together per region, each post weighted by its surveyed trips. The
estimates are this pipeline's own
([`estimate_fishery_indicators()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/estimate_fishery_indicators.md)),
the Timor portal's. Same 23 columns as the other countries; `gaul_2_*`
is filled on Atauro only, and `sampling_rate` and the FAO ARTFISH
columns stay empty.

## Usage

``` r
export_api_district_months(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The (standard Apache logj4) log level used as a threshold for the
  logging infrastructure. See
  [logger::log_levels](https://daroczig.github.io/logger/reference/log_levels.html)
  for more details.

## Value

no outputs. This function is used for its side effects.
