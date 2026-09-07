# Rename fields to match the portal ontology

Standardizes column names by applying a set of pattern-based
substitutions:

- `catch_price` -\> `revenue`

- `municipality` -\> `region`

- `landing_catch` -\> `landing_weight`

- `catch_preservation` -\> `conservation_place`

- `gear` -\> `gear_type`

## Usage

``` r
rename_ontology(x)
```

## Arguments

- x:

  A data frame-like object with named columns.

## Value

The same object `x`, with updated column names.

## Details

Uses [`gsub()`](https://rdrr.io/r/base/grep.html) over `names(x)`;
substitutions are applied sequentially. Note that replacing `gear` with
`gear_type` is pattern-based and will also affect any column name
containing the substring `"gear"`.

## Examples

``` r
if (FALSE) { # \dontrun{
df <- rename_ontology(df)
} # }
```
