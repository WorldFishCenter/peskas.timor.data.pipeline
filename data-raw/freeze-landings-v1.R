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
#
# NOTE (2026-09-05): `get_taxa_list()` changed under this script — it now takes
# its names from ASFIS and adds `taxa_search_aliases()`, so re-running would
# produce different length-length pools for TUN, SKH, LGE and CLP. That is
# harmless because the frozen snapshot is **already built** in both `timor` and
# `timor-dev` and is never rebuilt; if it ever has to be, re-freeze
# deliberately and expect v1's converted lengths to move. `filter_by_area`
# stays FALSE here on purpose: a length-length ratio is a body-proportion
# constant, unlike the length-weight pools this session filtered.
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

# --- Flatten to the long catch table ----------------------------------------
# Migration Phase 4 made the interchange format a flat long parquet, one row
# per (submission, catch, length bin). v1's own column reconciliation lives
# here rather than in the package: the form is dead, and `preprocess_landings()`
# should not carry a shape nothing will ever produce again.
labels <- survey_labels(conf)

long <- frozen %>%
  select(-dplyr::any_of(c("_attachments", "_bamboo_dataset_id"))) %>%
  mutate(
    fuel_L = NA_character_,
    reason_no_activity = .data$reason_for_zero_boats,
    catch_outcome = NA_character_
  ) %>%
  rename(`trip_group/habitat` = "trip_group/habitat_boat") %>%
  select(-"reason_for_zero_boats") %>%
  tidyr::unnest("species_group", keep_empty = TRUE) %>%
  tidyr::unnest("length_individuals", keep_empty = TRUE) %>%
  rename(
    n_catch = "n",
    length = "mean_length",
    catch_use = "food_or_sale"
  ) %>%
  mutate(
    survey_version = "v1",
    n_catch = as.integer(.data$n_catch) + 1L,
    submission_id = as.character(.data$`_id`),
    survey_id = .data$`_uuid`,
    landing_date = lubridate::as_date(.data$date),
    # Submission 16182387 was submitted 2017-12-14 recording a landing on
    # 2015-07-07, two years before the form existed. It is the only pre-2017
    # landing date in the whole pipeline, it has no catch, no individuals and no
    # revenue, and alert 10 already flags it. The date is not recoverable, so
    # drop it rather than publish it (decided 2026-08-10). Correcting it here
    # rather than in validation keeps it with v1's other frozen corrections.
    landing_date = dplyr::if_else(
      .data$submission_id == "16182387", as.Date(NA), .data$landing_date
    ),
    submission_date = lubridate::with_tz(
      lubridate::ymd_hms(.data$`_submission_time`), "Asia/Dili"
    ),
    landing_site_code = as.character(.data$landing_site_name),
    habitat_code = as.character(.data$`trip_group/habitat`),
    gear_code = as.character(.data$`trip_group/gear_type`),
    vessel_code = as.character(.data$`trip_group/boat_type`),
    trip_duration = as.numeric(.data$`trip_group/duration`),
    catch_price = as.numeric(.data$total_catch_value),
    tracker_imei = NA_character_,
    no_men_fishers = as.numeric(.data$`trip_group/no_fishers/no_men_fishers`),
    no_women_fishers = as.numeric(
      .data$`trip_group/no_fishers/no_women_fishers`
    ),
    no_child_fishers = as.numeric(
      .data$`trip_group/no_fishers/no_child_fishers`
    ),
    n_fishers = peskas.timor.data.pipeline:::sum_fishers(
      .data$no_men_fishers, .data$no_women_fishers, .data$no_child_fishers
    ),
    # The remaining standard columns validation reads, added in migration
    # Phase 5. Without them v1's 10,117 submissions arrive at
    # `validate_landings()` with no landing-site code and are all flagged 16,
    # and its gleaner and fuel values drop out of the outlier bounds the
    # thresholds are computed from.
    submitted_by = as.character(.data$`_submitted_by`),
    mesh_size = peskas.timor.data.pipeline:::mesh_size_mm(
      .data$`trip_group/mesh_size`,
      .data$`trip_group/mesh_size_other`
    ),
    n_gleaners = abs(as.numeric(.data$how_many_gleaners_today)),
    happiness = as.integer(.data$happiness_rating),
    # v1 asked about neither a boat flag, fuel, nor catch preservation. `fuel_L`
    # is NA above; the other two never existed on the form, and were NA for
    # every v1 row of the merged table before this too.
    has_boat = NA_character_,
    fuel = as.numeric(.data$fuel_L),
    conservation_code = NA_character_
  ) %>%
  peskas.timor.data.pipeline:::resolve_catch_taxa(labels) %>%
  peskas.timor.data.pipeline:::resolve_survey_labels(labels)

stopifnot(
  dplyr::n_distinct(long$submission_id) == nrow(v1),
  nrow(long) == nrow(after)
)
message("Frozen long table: ", nrow(long), " catch rows")

# --- Write ------------------------------------------------------------------
coasts::upload_parquet_to_cloud(
  data = long,
  prefix = conf$surveys$landings$v1$frozen$file_prefix,
  provider = conf$storage$google$key,
  options = coasts::resolve_storage_opts(conf, "country")
)
