# Extract summaries from one pds track

The unit of work
[`describe_pds_tracks()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/describe_pds_tracks.md)
maps over. See there for what the descriptors are and why they exist.

## Usage

``` r
get_tracks_descriptors(Trip, conf, tracks_list)
```

## Arguments

- Trip:

  A pds trip id.

- conf:

  The configuration file.

- tracks_list:

  Character vector of track object names in the PDS bucket, as returned
  by
  [`coasts::cloud_object_names()`](https://rdrr.io/pkg/coasts/man/cloud_object_names.html).

## Value

A one-row tibble of descriptors, or a zero-row tibble if the trip has no
stored track.
