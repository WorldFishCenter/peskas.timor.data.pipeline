# Build per-taxon length-weight coefficients

Fetches coefficients from FishBase and SeaLifeBase via
[`coasts::get_taxa_morphometrics()`](https://rdrr.io/pkg/coasts/man/get_taxa_morphometrics.html)
and collapses them to **one coefficient pair per taxon code**, the same
aggregation Mozambique uses: geometric mean of `a`, arithmetic mean of
`b`.

## Usage

``` r
get_morphometric_tables(conf)
```

## Arguments

- conf:

  The configuration file.

## Value

A list with `expanded` (the taxon-to-species expansion) and
`length_weight` (`alpha3_code`, `n_studies`, `lw_a`, `lw_b`).

## FAO area filtering

Coefficients are restricted to the FAO major fishing areas in
`conf$metadata$fishbase$fao_areas` — **57 and 71** for Timor-Leste,
which straddles the Eastern Indian Ocean and the Western Central
Pacific. Do not leave that key unset:
[`coasts::resolve_fao_areas()`](https://rdrr.io/pkg/coasts/man/resolve_fao_areas.html)
falls back to `c(51, 57)`, the *Indian* Ocean pair the WIO pipelines
use, which would filter Timor on one wrong area and miss the Pacific one
entirely, silently.

The filter was off until 2026-09-05, justified as "body form does not
stop applying at an area boundary". That answered the wrong question —
the filter removes *species* from the pool, it does not invalidate a
coefficient. The measured reason to have it on is that the unfiltered
pools are dominated by species that do not occur in Timor: `CLP` was
priced off 114 records of *Clupea harengus* (Atlantic herring) and 36 of
*Sprattus sprattus*.

`strip_parentheticals` is on because several FAO names carry a bracketed
synonym (`"Haemulidae (=Pomadasyidae)"`) and match nothing as written.

## Length types

A published pair `W = a * L^b` is fitted on whichever axis the study
used — `Type` is `TL`, `FL`, `SL`, `CL`, … — but every length reaching
[`join_weights()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/join_weights.md)
is a **total length**. Pooling the pairs as published therefore averaged
fork-length and standard-length fits into one number and applied it to
TL, which overestimates weight, because FL and SL are shorter than TL
(medians here: FL 0.958 TL, SL 0.827 TL).

`length_length`, fetched by the same call, restates every convertible
pair on a TL basis. POPLL fits `Length1 = aL + bL * Length2` — **the
second column is the predictor** — so the ratio `L_type / TL` is `bL`
when `Length2` is `TL` and `1 / bL` when `Length1` is. Substituting
`L_type ~= ratio * TL` into `W = a * L_type^b` gives
`W = a * ratio^b * TL^b`: **`b` is unchanged and only `a` is rescaled**.
Fits with an intercept above 1 cm are not proportional and are skipped;
per species and type the median ratio is used.

`length_types = NULL` is passed for this reason. The coasts default
keeps only `TL`/`FL` pairs, which would leave the 460 `SL` rows
unconvertible — and `SL` is where the error is largest.

Measured on Timor's taxa: of 1,648 non-TL pairs, 1,363 convert and 285
do not. **The 285 are kept as published rather than dropped.** Dropping
them takes `MOO` down 92% and `SFA` 78% — those taxa's fetched pairs
carry them, and what is left is a curated supplement fitted on a
different axis again. Zanzibar 4.9.0 converts only for taxa that would
otherwise have nothing; Timor converts everything it can, which is why
no `Type` filter is needed here (see `summarise_lw_coeffs()`).

## What is pooled in

Timor's 559 curated rows over 11 mostly-invertebrate codes, from
`curated_lw_coeffs()`. They carry no FAO area, so they are bound on
**after** the filter and are never area-restricted — and, being bound
after the conversion above, they are never restated either. That is
deliberate: their `Type` values (`CW`, `ShL`, `ML`, `CL`) are
invertebrate axes FishBase carries no conversion for, and field practice
measures those taxa on total length anyway.
