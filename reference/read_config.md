# Read configuration file

Reads the package configuration and adds some logging lines. Wrapped for
convenience.

## Usage

``` r
read_config()
```

## Value

the environment parameters

## Details

Environment variables are loaded from `.env` first (see
[`load_dotenv()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/load_dotenv.md)),
so local runs and CI resolve the *same* configuration branch and differ
only by `R_CONFIG_ACTIVE`. The former `local:` environment, which read
plaintext files from `auth/`, no longer exists.

The configuration file is `inst/config.yml`. `conf.yml` is still
accepted as a fallback so that an older installed copy of the package
keeps resolving.
