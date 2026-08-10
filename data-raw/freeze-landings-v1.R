# Freeze the v1 (SSF Landings) survey
# =============================================================================
#
# One-off script. Kept for provenance, not exported, never run by the pipeline.
#
# v1's last submission was 2020-08-28 and the form has been dead for six years,
# so from migration Phase 3 onwards it is neither ingested nor preprocessed.
# This script takes the last `timor-landings-v1_preprocessed__*.rds` produced
# by the retired `preprocess_legacy_landings()` and writes it back under
# `surveys.landings.v1.frozen.file_prefix`, which `merge_landings()` reads.
#
# It also does the one substantive conversion. **v1 records fork length**;
# v2 and v3 record total length. `join_weights()` used to carry that difference
# every run, converting FL -> TL through a FishBase length-length table
# refetched on each pipeline run — for 10,117 rows of a frozen source. Doing it
# once here let `summarise_ll_coeffs()` and `normalise_length_to_tl()` be
# deleted from the package, so the length-length logic lives below: this script
# is now its only consumer.
#
# Run once per environment, with the environment set beforehand:
#
#   R_CONFIG_ACTIVE=default    Rscript data-raw/freeze-landings-v1.R   # timor-dev
#   R_CONFIG_ACTIVE=production Rscript data-raw/freeze-landings-v1.R   # timor
#
# =============================================================================

devtools::load_all(".")
library(dplyr)

conf <- read_config()
message("Environment bucket: ", conf$storage$google$options$bucket)

# --- The source snapshot ----------------------------------------------------
v1 <- download_versioned_rds(
  prefix = conf$surveys$landings$v1$preprocessed$file_prefix,
  provider = conf$storage$google$key,
  options = coasts::resolve_storage_opts(conf, "country")
)

# 12,017 KoBo submissions (AUDIT §1) minus the 1,900 submissions of the older
# Kg-recording form variant that `clean_legacy_landings()` dropped.
stopifnot(nrow(v1) == 10117)

# --- FL -> TL conversion, keyed on the raw species number -------------------
# `species` in the nested `species_group` is the raw form code
# (`catch_types$catch_number`), and it must stay that way: `join_weights()`
# maps it to a taxon for every survey version at weight time.
metadata <- get_preprocessed_sheets(conf)

# Length-length coefficients, collapsed to one pair per taxon and conversion.
# The relationship is reciprocal, so the inverse of every published pair is
# added before averaging. This used to be `summarise_ll_coeffs()` in the
# package; it was deleted once nothing in the recurring pipeline converted a
# length any more.
ll <- coasts::get_taxa_morphometrics(
  get_taxa_list(conf),
  filter_by_area = FALSE,
  strip_parentheticals = TRUE
)$length_length

to_tl <- bind_rows(
  ll,
  ll %>%
    filter(!is.na(.data$bL), .data$bL != 0) %>%
    transmute(
      alpha3_code = .data$alpha3_code,
      Length1 = .data$Length2,
      Length2 = .data$Length1,
      aL = -.data$aL / .data$bL,
      bL = 1 / .data$bL
    )
) %>%
  filter(!is.na(.data$aL), !is.na(.data$bL), .data$Length1 == "TL") %>%
  group_by(.data$alpha3_code, .data$Length2) %>%
  summarise(aL = mean(.data$aL), bL = mean(.data$bL), .groups = "drop") %>%
  select(catch_taxon = "alpha3_code", length_type = "Length2", "aL", "bL")

conv <- metadata$catch_types %>%
  transmute(
    species = as.character(.data$catch_number),
    catch_taxon = .data$interagency_code,
    length_type = case_when(
      # The same overrides `join_weights()` applies.
      .data$interagency_code %in% c("OCZ", "SLV", "IAX", "MOO") ~ "TL",
      !is.na(.data$length_type) ~ .data$length_type,
      TRUE ~ "FL"
    )
  ) %>%
  left_join(to_tl, by = c("catch_taxon", "length_type")) %>%
  select("species", "aL", "bL") %>%
  distinct()

message(
  "Convertible taxa: ",
  sum(!is.na(conv$aL)),
  " of ",
  nrow(conv)
)

# Converted in place rather than through unnest()/nest(): a round trip turns
# the empty `species_group` tibbles of no-catch submissions into one all-NA
# row, which would be a schema change, not a freeze.
convert_lengths <- function(g) {
  if (is.null(g) || nrow(g) == 0) {
    return(g)
  }
  co <- conv[match(g$species, conv$species), c("aL", "bL")]
  g$length_individuals <- purrr::pmap(
    list(g$length_individuals, co$aL, co$bL),
    function(li, a, b) {
      if (is.null(li) || nrow(li) == 0 || is.na(a)) {
        return(li)
      }
      li$mean_length <- a + li$mean_length * b
      li
    }
  )
  g
}

frozen <- v1 %>%
  mutate(species_group = purrr::map(.data$species_group, convert_lengths))

# --- Assertions -------------------------------------------------------------
flat <- function(x) {
  x %>%
    tidyr::unnest("species_group", keep_empty = TRUE) %>%
    tidyr::unnest("length_individuals", keep_empty = TRUE)
}
before <- flat(v1)
after <- flat(frozen)

stopifnot(
  nrow(frozen) == nrow(v1),
  identical(names(frozen), names(v1)),
  nrow(after) == nrow(before),
  identical(before$species, after$species),
  identical(before$n_individuals, after$n_individuals)
)

changed <- which(
  !is.na(before$mean_length) & before$mean_length != after$mean_length
)
message(
  "Catch rows: ",
  nrow(before),
  " | lengths converted: ",
  length(changed),
  " (",
  round(100 * length(changed) / sum(!is.na(before$mean_length)), 1),
  "% of measured)"
)
message(
  "mean length before: ",
  round(mean(before$mean_length, na.rm = TRUE), 3),
  " after: ",
  round(mean(after$mean_length, na.rm = TRUE), 3)
)

# --- Write ------------------------------------------------------------------
# Stays `.rds`. The interchange format is still nested list-columns; arrow
# reads them back as `vctrs_list_of`, which `dplyr::bind_rows()` refuses to
# combine with the plain lists coming out of the v2/v3 `.rds` files in
# `merge_landings()`. Phase 4 flattens every source to long parquet and the
# question disappears.
filename <- add_version(
  conf$surveys$landings$v1$frozen$file_prefix,
  extension = "rds"
)
readr::write_rds(frozen, filename, compress = "gz")

coasts::upload_cloud_file(
  file = filename,
  provider = conf$storage$google$key,
  options = coasts::resolve_storage_opts(conf, "country")
)
message("Uploaded ", filename)
