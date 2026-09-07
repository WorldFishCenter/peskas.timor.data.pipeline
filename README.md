
<!-- README.md is generated from README.Rmd. Please edit that file -->

# peskas.timor.data.pipeline

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://www.tidyverse.org/lifecycle/#experimental)
[![Peskas Timor Data
Pipeline](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/actions/workflows/data-pipeline.yaml/badge.svg)](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/actions/workflows/data-pipeline.yaml)
[![R-CMD-check](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/actions/workflows/R-CMD-check.yaml)
[![Codecov test
coverage](https://codecov.io/gh/WorldFishCenter/peskas.timor.data.pipeline/branch/main/graph/badge.svg)](https://codecov.io/gh/WorldFishCenter/peskas.timor.data.pipeline?branch=main)
<!-- badges: end -->

peskas.timor.data.pipeline implements, deploys and executes the data and
modelling pipelines behind [Peskas](https://timor.peskas.org), the
small-scale fisheries analytics system of Timor-Leste.

It ingests KoBoToolbox landing surveys and Pelagic Data Systems (PDS)
GPS tracker data, preprocesses and validates them, models fishery
indicators, and publishes JSON to a public Google Cloud Storage bucket
that the live portal reads. It also publishes the cross-country API
tables that Peskas shares with the Kenya, Mozambique and Zanzibar
pipelines, deposits datasets on Harvard Dataverse, and emails reports.

## The pipeline is an R package

Structuring the pipeline as an R package makes it easier to write
production-grade software. Specifically, it allows us to:

- better handle system and package dependencies,
- forces us to split the code into functions,
- makes it easier to document the code, and
- makes it easier to test the code

We make heavy use of [tidyverse style
conventions](https://engineering-shiny.org) and the
[usethis](https://usethis.r-lib.org) package to automate tasks during
project setup and deployment.

For more information about the rationale of structuring a pipeline as a
package, see [Chapter
3](https://engineering-shiny.org/structuring-project.html#structuring-your-app_)
of [*Engineering Production-Grade Shiny
Apps*](https://engineering-shiny.org). The best place to learn more
about package development is the [*R packages*](https://r-pkgs.org) book
by Hadley Wickham and Jenny Bryan.

## Shared code lives in `peskas.coasts`

Everything that is not specific to Timor-Leste has moved to
[`peskas.coasts`](https://github.com/WorldFishCenter/peskas.coasts), the
hub the four Peskas country pipelines share: cloud storage, KoBo
retrieval, PDS ingestion, the Airtable reference frame, MongoDB access
and the taxa morphometrics. This package keeps what is genuinely Timor’s
— the survey reshaping of three form generations, sixteen validators,
the `glmmTMB` catch and revenue models, the nutrient and RDI
calculations, and the portal JSON contract.

`peskas.coasts` is not on CRAN. It is declared in `Remotes:` and, in the
production container, installed from the latest tagged release, which
the workflow resolves at build time and passes in as `COASTS_REF`.
**Version 4.6.0 is a hard floor.**

## The pipeline runs on GitHub Actions

Each step of the pipeline is a function in this package, and those
functions are deployed and connected using [GitHub
Actions](https://docs.github.com/en/actions/learn-github-actions).
Workflow functions take no arguments and are used for their side
effects.

The pipeline is defined in
[`.github/workflows/data-pipeline.yaml`](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/blob/main/.github/workflows/data-pipeline.yaml)
— every two days and on every push:

    build-container
    ├── ingest-preprocess-metadata-tables
    ├── ingest-landings ──▶ preprocess-landings
    └── ingest-pds-data ──▶ preprocess-pds-data ──▶ validate-pds-data

    merge-landings ──▶ validate-landings ──▶ merge-trips ──▶ model-indicators
                                                         └─▶ export-trips

The other workflows are `R-CMD-check`, `pkgdown`, `test-coverage`,
`pr-commands`, `release` (which cuts a GitHub release from `NEWS.md`),
and three scheduled Timor-specific jobs: the data report, the Dataverse
upload and the weekly validation email.

Artifacts produced by each job are written to cloud storage and read
back by the next job. They are versioned by `add_version()`, which
stamps a timestamp and the commit sha, so every artifact traces to a
unique run:

    <prefix>__<YYYYMMDDHHMMSS>_<git-sha7>__.<ext>

`coasts::cloud_object_name(version = "latest")` resolves the newest
version of a prefix, and `coasts::cloud_object_names()` enumerates a
whole family.

Interchange format is **flat long parquet** — one row per (submission,
catch, length bin) — from the raw survey table through to the validated
one.

## Environment parameters are in the config file

How the pipeline runs is specified in
[`inst/config.yml`](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/blob/main/inst/config.yml),
read by `read_config()`. Its shape follows the cross-country template
shipped as
[`inst/config_template.yml`](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/blob/main/inst/config_template.yml).
Keeping these parameters out of the code is what lets one codebase run
against two sets of cloud resources; we use the
[config](https://github.com/rstudio/config) package, which selects an
environment from `R_CONFIG_ACTIVE`.

There are two environments:

- **`default`** — the development environment. It uses the `-dev`
  buckets (`timor-dev`, `pds-timor-dev`, `public-timor-dev`,
  `peskas-api-dev`), so the whole pipeline can run end to end without
  touching anything the portal reads. This is what `.Renviron` sets, and
  what every push to a non-`main` branch runs in CI.
- **`production`** — the same code against the production buckets. The
  workflow sets `R_CONFIG_ACTIVE=production` only when the ref is
  `main`.

Credentials come from the environment in both cases, never from a file
in the repository. Copy
[`.env.example`](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/blob/main/.env.example)
to `.env` and fill it in for local work — `read_config()` loads it
through `load_dotenv()`. In CI the same variables are supplied from
GitHub secrets by the workflow’s `env:` block. `.env` is gitignored and
must stay that way.

Note that `read_config()` deliberately logs configuration **key names**
only. The resolved configuration holds the service-account key and every
token, and GitHub Actions masks only byte-exact matches of a registered
secret.

## We use docker containers

Docker makes it easier to run and develop the code.

- **Development:**
  [`Dockerfile`](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/blob/main/Dockerfile)
  is based on `rocker/geospatial` and spins up an RStudio server. Run
  `docker compose up` from the project directory and open
  <http://localhost:8802>.
- **Production:**
  [`Dockerfile.prod`](https://github.com/WorldFishCenter/peskas.timor.data.pipeline/blob/main/Dockerfile.prod)
  is what the pipeline runs in. The first job of the workflow builds it
  and every other job uses it, so each step executes in an identical
  environment.

Both need to know which `peskas.coasts` release to install, and neither
has a default, so a local build must pass it explicitly:

``` bash
docker build -f Dockerfile.prod --build-arg COASTS_REF=v4.6.0 -t peskas-timor .
```

## Tests

Two suites, with different jobs:

- `tests/testthat/` — unit tests over pure reshaping and schema logic.
  No network, no credentials. Run with `devtools::test()`.
- `inst/tinytest/` — four assertion suites over the real artifacts a run
  produces (validated landings, validated PDS trips, merged trips,
  public data). They run as steps **inside** the pipeline workflow,
  right after the job that writes what they assert on, so a schema
  regression stops the pipeline instead of reaching the portal.

``` r
tinytest::run_test_file(system.file("tinytest/test_validated_landings.R",
                                    package = "peskas.timor.data.pipeline"))
```

The portal discovers its files dynamically, which means a renamed or
dropped object silently disappears from the site.
`data-raw/compare-portal-json.R` is the gate for that: it asserts object
names, keys, nesting, column sets and column types against a reference
set, and reports per-column numeric summaries. Run it before changing
anything on the export path.

## Logging

We use the [logger](https://daroczig.github.io/logger/) package to log
events in production. Every workflow function takes a `log_threshold`
argument.
