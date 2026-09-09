# Add timestamp and sha string to a file name

An alternative to version data is to name it using the sha (unique
identifier) of the code using to generate or process the data and the
time at which the data was generated or processed. This function adds
this information, a version identifier, to a file name (character
string)

## Usage

``` r
add_version(filename, extension = "", sha_nchar = 7, sep = "__")
```

## Arguments

- filename:

  Path sans extension of the file to version

- extension:

  Extension of the file

- sha_nchar:

  Number of characters from the SHA to use as the version identifier

- sep:

  Characters separating the version identifier from the file name

## Value

A character string with the file name and the version identifier

## Details

The SHA information is retrieved using
[git2r::sha](https://docs.ropensci.org/git2r/reference/sha.html). If the
code is not running in a context aware of a git repository (for example
when code is running inside a container) then this function attempts to
get the sha from the environment variable `GITHUB_SHA`. If both of these
methods fail, no sha versioning is added.

## Examples

``` r
if (git2r::in_repository()) {
  add_version("my_file", "csv")
}
#> [1] "my_file__20260909071036_7518ccf__.csv"
```
