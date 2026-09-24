# Peskas Timor-Leste data pipeline

The code that turns landing surveys and boat GPS tracks from Timor-Leste
into the data behind Peskas Timor-Leste.

See the results at [timor.peskas.org](https://timor.peskas.org).
Timor-Leste also appears on
[coasts.peskas.org](https://coasts.peskas.org), the Peskas Coasts
regional comparison, which this pipeline feeds through a separate step.

## What it is

This pipeline collects, checks and models small-scale fisheries data for
Timor-Leste. It feeds Peskas Timor-Leste, the country’s official
national fisheries monitoring system, used by fisheries managers,
researchers and the public. It runs automatically every two days.

## What it produces

- Checks every landing survey and flags records that look wrong. The
  flags go to the Peskas Management Platform, and a weekly email lists
  the submissions with problems.
- Estimates the weight of each catch and the nutrients it provides.
- Estimates monthly catch and revenue for each municipality and for the
  whole country.
- Publishes the data files that timor.peskas.org reads, and a [data
  report](https://storage.googleapis.com/public-timor/data_report.html)
  twice a week.
- Sends Timor-Leste landing records to the Peskas Fishery Data API, and
  summaries to Peskas Coasts.
- Publishes the public dataset on Harvard Dataverse once a month.

## Where the data comes from

- **Landing surveys**: enumerators record landings around Timor-Leste
  with KoboToolbox. The survey form has changed twice since it started,
  and the pipeline reconciles the three versions.
- **GPS trackers (Pelagic Data Systems)** on part of the fleet, matched
  to the surveyed landings.
- **Reference lists** of boats, species and landing sites, including the
  number of registered boats in each area.
- **Length-weight formulas** from FishBase and SeaLifeBase, used to turn
  fish lengths into weights.

The data updates every two days. Known limits:

- National and municipal totals are estimates: models scale the surveyed
  landings up using the number of registered boats.
- GPS tracks cover only boats that carry a tracker.

Key terms:

- **Landing**: a boat’s return to shore with its catch, recorded by an
  enumerator.
- **Enumerator**: a trained data collector who records landings at
  landing sites.
- **KoboToolbox**: the free mobile survey app enumerators use to record
  landings.
- **GPS trackers (Pelagic Data Systems)**: small solar-powered devices
  on boats that record where they travel.

## Who runs it

Peskas Timor-Leste is a partnership, since 2016, between
[WorldFish](https://worldfishcenter.org/) and the Timor-Leste [Ministry
of Agriculture and Fisheries](https://maf.gov.tl/) (MAF), through its
Department of Fisheries, Aquaculture and Marine Resources. Since 2021 it
has been funded by the Government of Timor-Leste, with technical support
from WorldFish and Pelagic Data Systems Inc. Its development was
supported by the Royal Norwegian Embassy in Jakarta, the Minderoo
Foundation, the CGIAR Big Data Platform and the Schmidt Foundation.

For questions, write to <peskas.platform@gmail.com>.

## Part of Peskas

Peskas is WorldFish’s open-source platform for monitoring small-scale
fisheries (<https://peskas.org>).

- [Peskas Zanzibar](https://zanzibar.peskas.org), [Peskas
  Kenya](https://peskas-dashboard-kenya.vercel.app/en), [Peskas
  Mozambique](https://peskas-dashboard-mozambique.vercel.app): country
  dashboards
- [Peskas Timor-Leste](https://timor.peskas.org): Timor-Leste portal
- [Peskas Coasts](https://coasts.peskas.org): regional comparison across
  countries
- [Peskas Tracks](https://tracks.peskas.org): app for fishers to see
  their trips and log catches
- [Peskas Kenya BMU
  dashboard](https://digitalfisheries.kenya.peskas.org): dashboard for
  Beach Management Units in Kenya
- [Peskas Management Platform](https://validation.peskas.org): data
  review and download for survey teams
- [Peskas Fishery Data API](https://api.peskas.org/docs): programmatic
  access to landing data
- Data pipelines:
  [Kenya](https://github.com/WorldFishCenter/peskas.kenya.data.pipeline),
  [Zanzibar](https://github.com/WorldFishCenter/peskas.zanzibar.data.pipeline),
  [Mozambique](https://github.com/WorldFishCenter/peskas.mozambique.data.pipeline),
  [Coasts](https://github.com/WorldFishCenter/peskas.coasts)

## For developers

### The pipeline is an R package

Each step is an exported function, and the GitHub Actions workflow calls
them in order. Everything not specific to Timor-Leste (cloud storage,
KoboToolbox retrieval, GPS tracker ingestion, reference lists, MongoDB
access, species weights) lives in
[peskas.coasts](https://github.com/WorldFishCenter/peskas.coasts); call
it as `coasts::fn()`. This package keeps the survey reshaping, the
validators, the catch and revenue models, the nutrient calculations and
the portal file contract. The function reference is at
<https://worldfishcenter.github.io/peskas.timor.data.pipeline/>.

peskas.coasts is not on CRAN. It is declared in `Remotes:`, and the
production container installs the latest coasts release; see
`DESCRIPTION` for the minimum version.

``` r

# Copy .env.example to .env and fill in the values (ask the Peskas team).
remotes::install_deps(dependencies = TRUE)  # installs peskas.coasts from GitHub
devtools::load_all()
devtools::document()    # rebuild man/ after changing roxygen comments
devtools::check()
```

### Configuration

[`inst/config.yml`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/inst/config.yml),
read by
[`read_config()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/read_config.md),
has two profiles:

- **`default`**: development. It uses the `-dev` buckets (`timor-dev`,
  `pds-timor-dev`, `public-timor-dev`, `peskas-api-dev`), so the whole
  pipeline can run without touching anything the portal reads.
  `.Renviron` sets it for local sessions, and every push to a branch
  other than `main` runs it in CI.
- **`production`**: the same code against the production buckets. The
  workflow selects it only on `main`. Never write to a production bucket
  from a local session or an ad-hoc script.

### Secrets and logging

Credentials come from the environment, never from a file in the
repository. Copy
[`.env.example`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/.env.example)
to `.env` for local work;
[`read_config()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/read_config.md)
loads it. In CI the same variables come from GitHub secrets. `.env` is
gitignored.

The resolved config holds the service-account key and every token, and
GitHub Actions masks only exact matches of a secret.
[`read_config()`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/reference/read_config.md)
therefore logs key names only; log single values you need, never the
whole config. Logging uses the
[logger](https://daroczig.github.io/logger/) package: pass
`log_threshold = logger::INFO` to every `coasts::` call in a workflow.

### How it runs in production

[`.github/workflows/data-pipeline.yaml`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/.github/workflows/data-pipeline.yaml)
is the only workflow that produces data. It runs every two days and on
every push; read it for the exact `needs:` graph. In outline:

``` R
build-container
├── ingest-landings -> preprocess-landings -> merge-landings -> validate-landings
└── ingest-pds-data -> preprocess-pds-data -> validate-pds-data

validate-landings ├── export-api
                  └── merge-trips (also needs validate-pds-data)
                        ├── model-indicators
                        └── export-trips (also needs model-indicators)

export-api + preprocess-pds-data -> summarize-model-data -> export-surveys-portal
```

`export-trips` writes the files timor.peskas.org reads.
`summarize-model-data` and `export-surveys-portal` are the shared coasts
steps that put Timor-Leste on Peskas Coasts; neither feeds
timor.peskas.org.

Three scheduled workflows run `main`’s code in the container the
pipeline builds: `data-report.yaml` (Mondays and Thursdays),
`validation-email-sender.yaml` (Mondays) and `dataverse-upload.yaml`
(first of each month).

Files the jobs write are versioned as
`<prefix>__<YYYYMMDDHHMMSS>_<git-sha7>__.<ext>`, so each one traces back
to a single run.

### Docker

[`Dockerfile.prod`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/Dockerfile.prod)
is the image the pipeline runs in. The first workflow job builds it and
every other job uses it. It installs peskas.coasts at the `COASTS_REF`
build argument, which has no default:

``` bash
docker build -f Dockerfile.prod --build-arg COASTS_REF=<coasts release tag> -t peskas-timor .
```

### Tests

- `tests/testthat/`: unit tests over reshaping and schema logic, with no
  network or credentials. Run `devtools::test()`.
- `inst/tinytest/`: six assertion suites over the real files a run
  produces (validated landings, validated GPS trips, Peskas Coasts
  summaries, merged trips, the portal contract and public data). They
  run as steps inside the pipeline, right after the job that writes what
  they check, so a schema change stops the pipeline before it reaches
  the portal.

The portal finds its files by name, so a renamed or dropped file
disappears from the site without an error.
`inst/tinytest/test_portal_contract.R` guards this on every run; run it
before changing anything on the export path:

``` r

tinytest::run_test_file(system.file("tinytest/test_portal_contract.R",
                                    package = "peskas.timor.data.pipeline"))
```

### Releases

Bump `Version:` in `DESCRIPTION` and add a block at the top of
[`NEWS.md`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/NEWS.md).
On a push to `main`, `release.yaml` turns that block into a GitHub
release.

### AI-assisted work

See
[`CLAUDE.md`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/CLAUDE.md)
and `.claude/rules/`.

## Licence

GPL-3. See
[`LICENSE.md`](https://worldfishcenter.github.io/peskas.timor.data.pipeline/LICENSE.md).
