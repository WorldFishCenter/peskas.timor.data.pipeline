# PDS-derived map products

Everything in this file is Timor-only and downstream of PDS, not PDS
ingestion: migration Phase 7 deleted `ingest_pds_trips()`,
`ingest_pds_tracks()` and `preprocess_pds_trips()` in favour of
[`coasts::ingest_pds_trips()`](https://rdrr.io/pkg/coasts/man/ingest_pds_trips.html),
[`coasts::ingest_pds_tracks()`](https://rdrr.io/pkg/coasts/man/ingest_pds_tracks.html)
and
[`coasts::preprocess_pds_tracks()`](https://rdrr.io/pkg/coasts/man/preprocess_pds_tracks.html),
which is how Mozambique, Kenya and Zanzibar have always done it — none
of them carries any PDS code.

## Details

What is left are the boundary and taxa-name lookups the map products
used to share with
[`format_public_data()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/format_public_data.md).

Phase 8 resolved the dangling dependency the Phase 7 handover described.
Of the two map products, `tracks-map.png` (last written 2021-12-11)
turned out to have no reader at all, and `indicators_gridded.rds` (last
written 2024-07-27 in production) had exactly one:
[`export_files()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/export_files.md),
which rebuilt `portal-indicators_grid.json` and
`portal-label_groups_list.json` from it on every run. Those are the two
objects `peskas.timor.portal.v2` explicitly excludes, so Phase 8 dropped
them — which left `ingest_pds_map()`, `ingest_kepler_tracks()`,
`kepler_mapper()` and `ingest_complete_tracks()` unreferenced. **Phase
11 deleted all four**, with `inst/kepler_mapper.py` and the accessors
that fed them. What is left is
[`get_timor_boundaries()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_timor_boundaries.md)
and
[`convert_taxa_names()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/convert_taxa_names.md),
both of which
[`format_public_data()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/format_public_data.md)
calls.
