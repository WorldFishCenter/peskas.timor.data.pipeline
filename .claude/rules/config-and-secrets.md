---
paths:
  - "inst/config*.yml"
  - ".github/workflows/**"
  - "R/utils.R"
  - ".env.example"
---

# Configuration and secrets

`read_config()` calls `load_dotenv()` then
`config::get(config = Sys.getenv("R_CONFIG_ACTIVE", "default"))`. Two
environments:

| env | storage buckets | api / hub buckets |
|---|---|---|
| `default` | `timor-dev`, `pds-timor-dev`, `public-timor-dev` | `peskas-api-dev`, `peskas-coasts-dev` |
| `production` | `timor`, `pds-timor`, `public-timor` | `peskas-api-prod`, `peskas-coasts` |

`.Renviron` pins `default` **and overrides the command line**, so a bare
`R_CONFIG_ACTIVE=production Rscript …` silently targets dev; use
`R_ENVIRON_USER=/dev/null` as well. CI sets `production` only on `main` — any
push to another branch runs the whole pipeline against `-dev`, which is the
integration test.

**`coasts` reads this file too**, through
`read_config(package = "peskas.timor.data.pipeline")`. Grep the hub as well as
`R/` before removing a key.

**Never log the resolved config.** `read_config()` used to end with
`log_debug("Running with parameters {pars}")` and every workflow function
defaults to `log_threshold = logger::DEBUG`, so each CI job printed the
service-account key, the Airtable PAT, the Dataverse token and the blastula
credentials into its log. Actions masks only byte-exact matches of a registered
secret, which re-serialised JSON is not. **Every `coasts::` workflow call in
`data-pipeline.yaml` must pass `log_threshold = logger::INFO`** —
`coasts::read_config()` had the same defect (fixed upstream; the explicit
threshold stays as a regression guard).

**Nothing walks the children of `storage` or `pds_storage`** — always address a
provider explicitly as `conf$storage$google`. Six call sites used to do
`purrr::map(conf$storage, ~ upload_cloud_file(files, .$key, .$options))`, which
treated every child of `storage` as a storage *provider*. That is what makes it
safe for `storage.mongodb` to sit beside it.

## Keys that carry a trap

- **`metadata.fishbase.fao_areas` must exist.** `coasts::resolve_fao_areas()`
  falls back to `c(51, 57)`, the Indian Ocean pair the WIO repos use, so an
  unset key filters Timor on one wrong area and misses the Pacific one
  silently.
- **`metadata.fishbase.db_version` must stay pinned.** See
  `.claude/rules/taxa-and-weights.md`.
- **`metadata.coast_areas`** is the one site→coast table, 40 entries keyed on
  the frame's site names. Coast is a property of the **landing site**, not the
  municipality: two municipalities hold sites on both coasts (Lautem 4,299
  north / 2 south, Manatuto 2,919 / 1) and latitude cannot derive it, because
  Oecusse is an exclave whose north-facing sites sit further south than most of
  the south coast. `get_summary_data()` reduces it to a municipality→coast map
  by **submission-weighted majority** — not site-count-weighted, under which
  Manatuto is 1 north / 2 south and would flip, changing `estimated_revenue`.
  A site missing from the table warns; `export_files()` hard-fails if the
  derived map is absent.
- **`metadata.tracker_imeis`** is the IMEI roster. See
  `.claude/rules/validation.md`.
- **`pds.exclude_customers`** is a denylist on purpose. See
  `.claude/rules/pds.md`.
- `inst/config_template.yml` is documentation, not read at runtime. Keep its key
  set identical to `config.yml`'s — it had drifted by sixteen keys before.

## Environment variables

`.env` locally (template `.env.example`), workflow environment in CI. Both
JSON-valued entries must be minified onto one line — dotenv parses line by line.

| env var | GitHub secret | notes |
|---|---|---|
| `KOBO_USERNAME` / `KOBO_PASSWORD` | same | basic auth against `eu.kobotoolbox.org` |
| `KOBO_TOKEN` | *not set in CI* | optional; `ingestion` uses basic auth |
| `KOBO_ASSET_ID_V1/2/3` | `KOBO_PESKAS1/2/3`, mapped in the workflow `env:` block | the secrets keep their legacy names on purpose: secret values are write-only, so renaming means re-entering three asset ids by hand for no behavioural gain |
| `GCP_SA_KEY` | `PESKAS_DATAINGESTION_GCS_KEY` | full service-account JSON, minified |
| `MONGODB_CONNECTION_STRING_VALIDATION` | *not set in CI yet* | absent → `validate_landings()` warns and only the GCS snapshot is written |
| `PDS_TOKEN` / `PDS_SECRET` | `PESKAS_PDS_TOKEN` / `PESKAS_PDS_SECRET` | |
| `DATAVERSE_TOKEN` | `PESKAS_DATAVERSE_TOKEN` | |
| `PESKAS_GMAIL_KEY` | same | serialized blastula credentials JSON, **not** a bare app password |
| `AIRTABLE_TOKEN` | same | the **bare** `pat…` — coasts prepends `Bearer `. Needs frame-base read + `schema.bases:read` |
| `AIRTABLE_BASE_ID_FRAME` | same | PESKAS \| FRAME, `appMMEJYlJdfSJEjm` |

Three stale GitHub secrets survive, mapped by no workflow; deleting them is a
user action: `AIRTABLE_KEY`, `VALID_SHEET_ID`, `GOOGLE_SHEET_ID`.
