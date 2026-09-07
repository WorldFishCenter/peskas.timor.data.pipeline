# Get taxa nutritional values

Download the modelled nutrional properties of each taxa according to
Hicks et al., 2019
(<https://www.nature.com/articles/s41586-019-1592-6>). The table of
nutrients for each taxon is directly called from the paper repository
(<https://github.com/mamacneil/NutrientFishbase>).

## Usage

``` r
get_nutrients_table(conf, expanded = NULL, summarise = TRUE, convert = TRUE)
```

## Arguments

- conf:

  The configuration file.

- expanded:

  Optional pre-computed taxon-to-species expansion from
  [`get_morphometric_tables()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/get_morphometric_tables.md),
  to avoid a second FishBase round-trip.

- summarise:

  Whether you want to summarise the nutritional values for each group.
  The function use the median by default.

- convert:

  Whether you want to convert the nutritional values units to grams. See
  details for the default starting units.

## Value

A data frame with nutritional values for each taxa group.

## Details

Starting units for each nutrient:

- mg/100g: Calcium, Iron and Zinc.

- μg/100g: Selenium and Vitamin A.

- g/100g: Omega 3 and Protein.
