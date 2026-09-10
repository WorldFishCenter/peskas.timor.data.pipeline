library(peskas.timor.data.pipeline)

logger::log_threshold(logger::ERROR)
# Local runs read their credentials from `.env`; CI supplies them as real
# environment variables, where this is a no-op.
if (file.exists(".env")) dotenv::load_dot_env()

conf <- coasts::read_config(package = "peskas.timor.data.pipeline")

# `coasts::export_geos()` builds the portal's metrics collections from one
# `<country>_monthly_summaries_map` per country and joins polygons to metrics on
# `country_gaul1_name`. Every failure mode below renders as polygons with no
# data -- visually identical to Timor having no export at all -- so none of it
# announces itself in a pipeline log.
metrics <- coasts::download_parquet_from_cloud(
  prefix = "timor_monthly_summaries_map",
  provider = conf$storage$google$key,
  options = conf$storage$google$options_coasts
)

# The column contract export_geos() reads. A rename drops Timor silently.
expect_equal(
  names(metrics),
  c(
    "country", "gaul1_name", "gaul_2_name", "date",
    "mean_cpue", "mean_rpue", "mean_price_kg"
  ),
  info = "the coasts portal metrics schema"
)

# The join key. `timor`, not `Timor` or `timor-leste`.
expect_equal(
  unique(metrics$country),
  conf$country,
  info = "country is the portal join key and must be the config value"
)

# A parquet of the right shape carrying nothing is the bug this whole export
# exists to fix, so assert the metrics are actually populated rather than
# merely present.
for (metric in c("mean_cpue", "mean_rpue", "mean_price_kg")) {
  expect_true(
    sum(!is.na(metrics[[metric]])) > 0,
    info = paste(metric, "has at least one non-missing value")
  )
  # A trip recorded with zero fishers divides catch by zero. `Inf` survives
  # mean() and median() into the portal, where no axis can plot it.
  expect_true(
    !any(is.infinite(metrics[[metric]])),
    info = paste(metric, "is finite where it is not missing")
  )
}

# The gaul_2_name -> gaul2_name join against `TLS_boundaries_gaul2`.
# `gaul1_name` comes only from that join, so a post the boundary file does not
# know arrives here with a null municipality -- which is the same thing the
# portal sees, and cheaper to assert than re-reading the 9.6 MB geojson.
#
# Rows with no district at all are expected and are not a join failure: Kenya
# and Mozambique publish them too. Only a *named* post must always resolve.
named <- metrics[!is.na(metrics$gaul_2_name), ]
unmatched <- unique(named$gaul_2_name[is.na(named$gaul1_name)])
expect_true(
  length(unmatched) == 0,
  info = paste0(
    length(unmatched), " administrative post(s) matched no polygon in ",
    conf$metadata$map_boundaries$gaul2, ": ",
    paste(unmatched, collapse = ", ")
  )
)
