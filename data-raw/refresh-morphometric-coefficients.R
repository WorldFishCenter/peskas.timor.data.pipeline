# Refresh inst/extdata/morphometric-coefficients.csv from the Google Sheets
# `morphometric_table`.
#
# The 559 curated rows are Timor's own length-weight coefficients for 11 mostly
# invertebrate codes (COZ CRA CUX FLY GZP IAX MOO OCZ PEZ SFA SLV, 98 species)
# that FishBase and SeaLifeBase do not carry. They are ~4% of national catch
# weight and `get_morphometric_tables()` pools them with the FishBase fetch.
#
# They used to be read from the Sheets on every pipeline run. They are a
# hand-curated reference table, not survey data — it changes when somebody
# edits it deliberately, which is not on the pipeline's schedule — so since the
# 2026-09-05 taxa-path session the package carries a snapshot and
# `calculate_weights()` reads no Google Sheet at all.
#
# Their permanent home is a curated table in `peskas.coasts` that
# `get_taxa_morphometrics()` pools automatically, so that every country can put
# rows there. That is COASTS-TODO C23; this CSV is the interim.
#
# Run this only after deliberately editing the Sheet, and commit the diff:
#
#   R_ENVIRON_USER=/dev/null Rscript data-raw/refresh-morphometric-coefficients.R
#
# The `morphometric_table` entry has to be put back in
# `metadata.google_sheets.tables` for the run, since nothing reads it any more.

library(dplyr)
devtools::load_all()

conf <- read_config()
coasts::cloud_storage_authenticate(
  conf$storage$google$key,
  conf$storage$google$options
)

sheet <- get_preprocessed_sheets(conf)$morphometric_table
stopifnot(!is.null(sheet))

out <- sheet %>%
  transmute(
    alpha3_code = as.character(.data$interagency_code),
    species = as.character(.data$Species),
    type = as.character(.data$Type),
    esq = as.character(.data$EsQ),
    a = as.double(.data$a),
    b = as.double(.data$b)
  ) %>%
  arrange(.data$alpha3_code, .data$species)

# Every row must carry a usable pair — `summarise_lw_coeffs()` drops the rest
# silently, and a curated table with silent holes is worse than none.
stopifnot(
  nrow(out) > 0,
  !anyNA(out$alpha3_code),
  !anyNA(out$a),
  !anyNA(out$b),
  all(out$a > 0)
)

write.csv(
  out,
  "inst/extdata/morphometric-coefficients.csv",
  row.names = FALSE,
  na = ""
)

message(
  nrow(out), " rows, ", dplyr::n_distinct(out$alpha3_code), " codes, ",
  dplyr::n_distinct(out$species), " species written"
)
