# Load environment variables from a .env file

Loads environment variables from a `.env` file if one exists. Called by
[`read_config()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/read_config.md)
before the configuration is resolved, so that the
`!expr Sys.getenv(...)` entries in `inst/config.yml` see the local
secrets.

## Usage

``` r
load_dotenv(file = ".env")
```

## Arguments

- file:

  Path to the `.env` file. Defaults to `.env` in the working directory.

## Value

`NULL`, invisibly. Called for its side effect.

## Details

In CI the variables come from the workflow environment and no `.env`
file is present, so this is a no-op there. Values already set in the
environment win:
[`dotenv::load_dot_env()`](https://rdrr.io/pkg/dotenv/man/load_dot_env.html)
does not overwrite them.
