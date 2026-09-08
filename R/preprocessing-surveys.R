#' Pre-process the landing surveys into a flat long catch table
#'
#' Reads the raw parquet written by [ingest_landings()] and turns each form
#' version into **one row per (submission, catch, length bin)** — the flat long
#' shape the other country pipelines use — resolving every code the form records
#' into its standard label along the way.
#'
#' The parameters needed in `config.yml` are:
#'
#' ```
#' surveys:
#'   landings:
#'     <version>:
#'       raw:
#'         file_prefix:
#'         version:
#'       preprocessed:
#'         file_prefix:
#' storage:
#'   google:
#'     key:
#'     options:
#' ```
#'
#' @section Column set:
#' The table is a **superset**. Alongside the standard columns
#' (`submission_id`, `landing_date`, `gaul_*`, `landing_site`, `n_fishers`,
#' `trip_duration`, `gear`, `vessel_type`, `habitat`, `catch_outcome`,
#' `n_catch`, `catch_taxon`, `scientific_name`, `length`, `catch_price`,
#' `mesh_size`, `n_gleaners`, `fuel`, `happiness`, …).
#'
#' The raw form codes are kept beside their resolved labels
#' (`landing_site_code`, `gear_code`, `vessel_code`, `habitat_code`) because
#' validation needs both: an unrecognised code is exactly what alerts 12, 14,
#' 16 and 19 report.
#'
#' @section Labels:
#' Taxa, gear, vessels and landing sites are resolved from the Airtable frame
#' via [get_assets()]. Habitat is a fixed code-to-label lookup.
#'
#' @param versions Character vector of form versions to pre-process. Defaults to
#'   the live forms, `c("v2", "v3")`. `v1` is frozen — see
#'   a one-off freeze script, out of band.
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels].
#'
#' @return No output. This function is used for its side effects.
#' @keywords workflow preprocessing
#' @export
preprocess_landings <- function(versions = c("v2", "v3"),
                                log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  labels <- survey_labels(conf)

  purrr::walk(versions, function(v) {
    logger::log_info("Downloading raw landings {v}...")
    raw <- coasts::download_parquet_from_cloud(
      prefix = conf$surveys$landings[[v]]$raw$file_prefix,
      provider = conf$storage$google$key,
      options = coasts::resolve_storage_opts(conf, "country"),
      version = conf$surveys$landings[[v]]$raw$version
    )

    logger::log_info("Reshaping {nrow(raw)} submissions of {v}...")
    preprocessed <- reshape_landings(raw, version = v, labels = labels)

    logger::log_info(
      "Uploading {nrow(preprocessed)} catch rows for {v} to cloud storage"
    )
    coasts::upload_parquet_to_cloud(
      data = preprocessed,
      prefix = conf$surveys$landings[[v]]$preprocessed$file_prefix,
      provider = conf$storage$google$key,
      options = coasts::resolve_storage_opts(conf, "country")
    )
  })
}

#' Merge the pre-processed landings of every form version
#'
#' Binds the long v2 and v3 tables with the frozen v1 snapshot. Column
#' reconciliation across form versions now happens in [preprocess_landings()],
#' one version at a time, so this is a plain bind.
#'
#' The parameters needed in `config.yml` are:
#'
#' ```
#' surveys:
#'   landings:
#'     v1:
#'       frozen:
#'         file_prefix:
#'         version:
#'     v2:
#'       preprocessed:
#'         file_prefix:
#'         version:
#'     v3:
#'       preprocessed:
#'         file_prefix:
#'         version:
#'     merged:
#'       file_prefix:
#' storage:
#'   google:
#'     key:
#'     options:
#' ```
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels].
#'
#' @return No output. This function is used for its side effects.
#' @keywords workflow preprocessing
#' @export
merge_landings <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  read_source <- function(prefix, version) {
    logger::log_info("Retrieving {prefix}...")
    coasts::download_parquet_from_cloud(
      prefix = prefix,
      provider = conf$storage$google$key,
      options = coasts::resolve_storage_opts(conf, "country"),
      version = version
    )
  }

  merged <- dplyr::bind_rows(
    # v1 is frozen: one snapshot of the last pre-processed output, reshaped and
    # with its fork lengths already converted to total length. See
    # the one-off freeze, out of band.
    read_source(
      conf$surveys$landings$v1$frozen$file_prefix,
      conf$surveys$landings$v1$frozen$version
    ),
    read_source(
      conf$surveys$landings$v2$preprocessed$file_prefix,
      conf$surveys$landings$v2$preprocessed$version
    ),
    read_source(
      conf$surveys$landings$v3$preprocessed$file_prefix,
      conf$surveys$landings$v3$preprocessed$version
    )
  )

  # The frozen v1 snapshot still carries the raw KoBo columns, so it is narrowed
  # here as well. `any_of()` because that snapshot predates any later column.
  merged <- dplyr::select(merged, dplyr::any_of(landing_cols()))

  logger::log_info(
    "Merged {dplyr::n_distinct(merged$submission_id)} submissions, ",
    "{nrow(merged)} catch rows, {ncol(merged)} columns"
  )
  coasts::upload_parquet_to_cloud(
    data = merged,
    prefix = conf$surveys$landings$merged$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country")
  )
}

# The standard columns of the preprocessed landings table, and the only ones
# stored. The raw KoBo columns are consumed to derive these and then dropped.
landing_cols <- function() {
  c(
    # submission
    "submission_id", "survey_id", "survey_version", "submitted_by",
    "landing_date", "submission_date",
    # place
    "landing_site", "landing_site_code",
    "gaul_1_code", "gaul_1_name", "gaul_2_code", "gaul_2_name",
    # effort
    "n_fishers", "no_men_fishers", "no_women_fishers", "no_child_fishers",
    "trip_duration", "gear", "gear_code", "vessel_type", "vessel_code",
    "habitat", "habitat_code", "has_boat", "mesh_size", "n_gleaners", "fuel",
    "conservation_code", "happiness", "tracker_imei",
    # catch
    "catch_price", "catch_outcome", "n_catch", "catch_taxon",
    "scientific_name", "catch_use", "length", "n_individuals"
  )
}

#' Reshape one raw landings table into the flat long catch table
#'
#' The whole of preprocessing for a single form version, as a pure function so
#' it can be exercised without cloud storage.
#'
#' @param raw A raw landings table as written by [ingest_landings()].
#' @param version Form version, `"v2"` or `"v3"`.
#' @param labels Output of [survey_labels()].
#'
#' @return A tibble, one row per (submission, catch, length bin).
#' @keywords preprocessing
#' @export
reshape_landings <- function(raw, version, labels) {
  submissions <- harmonise_submissions(raw, version)

  catches <- raw %>%
    reshape_species_groups() %>%
    # v3 offers the enumerator a stock picture to identify the catch; it is not
    # an observation.
    dplyr::select(-dplyr::any_of("stock_photo")) %>%
    expand_length_frequency()

  if (version == "v3") {
    # v3 asks "food or sale" once for the whole trip rather than per catch, and
    # a recorded "no catch" outcome overrides whatever the catch block holds.
    catches <- catches %>%
      dplyr::select(-dplyr::any_of("food_or_sale")) %>%
      dplyr::left_join(
        dplyr::select(submissions, "submission_id", "catch_use", "catch_outcome"),
        by = "submission_id"
      ) %>%
      dplyr::mutate(
        species = dplyr::if_else(
          !is.na(.data$catch_outcome) & .data$catch_outcome == "0",
          "0",
          .data$species
        )
      ) %>%
      dplyr::select(-"catch_outcome")
  } else {
    catches <- dplyr::rename(catches, catch_use = "food_or_sale")
  }

  submissions %>%
    dplyr::select(-dplyr::any_of("catch_use")) %>%
    # A left join is `unnest(keep_empty = TRUE)`: submissions with no catch keep
    # their row and get an empty catch.
    dplyr::left_join(catches, by = "submission_id") %>%
    resolve_catch_taxa(labels) %>%
    resolve_survey_labels(labels) %>%
    trim_free_text() %>%
        dplyr::select(dplyr::all_of(landing_cols()))
}

#' Reconcile one form version's submission-level columns
#'
#' Coalesces the differently-spelled KoBo questions of each form version into
#' one set of names, then derives the standard columns from them.
#'
#' @param raw A raw landings table.
#' @param version Form version, `"v2"` or `"v3"`.
#' @return A tibble, one row per submission.
#' @keywords preprocessing
#' @noRd
harmonise_submissions <- function(raw, version) {
  x <- raw %>%
    dplyr::select(
      -dplyr::starts_with("species_group"),
      -dplyr::starts_with("_attachments"),
      # All NA, and dropped after the merge for as long as it has existed.
      -dplyr::any_of("_bamboo_dataset_id")
    ) %>%
    dplyr::mutate(`_id` = as.character(.data$`_id`))

  x <- switch(version,
    v2 = harmonise_v2(x),
    v3 = harmonise_v3(x),
    stop("Unsupported landings version: ", version)
  )

  # Not every form asks every question, and validation reads the standard
  # columns below off both. Absent answers used to become NA when the versions
  # were bound together; now they become NA one version earlier.
  x <- add_missing_cols(x, c(
    "trip_group/has_boat", "trip_group/mesh_size", "trip_group/mesh_size_other",
    "how_many_gleaners_today", "group_conservation_trading/conservation",
    "happiness_rating"
  ))

  x %>%
    dplyr::mutate(
      survey_version = version,
      submission_id = .data$`_id`,
      survey_id = .data$`_uuid`,
      landing_date = lubridate::as_date(.data$date),
      submission_date = lubridate::with_tz(
        lubridate::ymd_hms(.data$`_submission_time`), "Asia/Dili"
      ),
      landing_site_code = as.character(.data$landing_site_name),
      habitat_code = as.character(.data$`trip_group/habitat`),
      gear_code = as.character(.data$`trip_group/gear_type`),
      vessel_code = as.character(.data$`trip_group/boat_type`),
      trip_duration = as.numeric(.data$`trip_group/duration`),
      catch_price = as.numeric(.data$total_catch_value),
      tracker_imei = as.character(.data$`trip_group/IMEI`),
      no_men_fishers = as.numeric(.data$`trip_group/no_fishers/no_men_fishers`),
      no_women_fishers = as.numeric(
        .data$`trip_group/no_fishers/no_women_fishers`
      ),
      no_child_fishers = as.numeric(
        .data$`trip_group/no_fishers/no_child_fishers`
      ),
      n_fishers = sum_fishers(
        .data$no_men_fishers, .data$no_women_fishers, .data$no_child_fishers
      ),
      # The remaining survey answers validation reads. They have no equivalent
      # in the PESKAS | FRAME frame and no standard name in the cross-country
      # template either, so preprocessing is simply where their coercion
      # belongs — the validators used to do it themselves off the raw KoBo
      # names.
      submitted_by = as.character(.data$`_submitted_by`),
      has_boat = as.character(.data$`trip_group/has_boat`),
      mesh_size = mesh_size_mm(
        .data$`trip_group/mesh_size`,
        .data$`trip_group/mesh_size_other`
      ),
      n_gleaners = abs(as.numeric(.data$how_many_gleaners_today)),
      fuel = as.numeric(.data$fuel_L),
      conservation_code = as.character(
        .data$`group_conservation_trading/conservation`
      ),
      happiness = as.integer(.data$happiness_rating)
    )
}

# Every KoBo answer arrives as character, so an absent question is an absent
# character column.
add_missing_cols <- function(x, cols) {
  missing <- setdiff(cols, names(x))
  if (length(missing) == 0) {
    return(x)
  }
  x[missing] <- NA_character_
  x
}

# The form offers a pick-list of imperial mesh sizes plus a free-text "other"
# ("seluk" is Tetum for "other", recorded in the pick-list as well). Result is
# millimetres, which is the unit the validated artefact has always published.
mesh_size_mm <- function(picked, other) {
  picked <- dplyr::if_else(picked == "seluk", NA_character_, picked)
  as.numeric(dplyr::coalesce(picked, other)) * 25.4
}

harmonise_v2 <- function(x) {
  x %>%
    dplyr::mutate(
      fuel_L = dplyr::coalesce(
        .data$`trip_group/Total_litru_mina_hir_e_ebe_gastu_ba_peska`,
        .data$`trip_group/fuel_used_L`
      ),
      # The no-boat habitat question is a multi-select; only the first choice
      # has ever been kept.
      habitat_no_boat = substr(
        dplyr::coalesce(
          .data$`trip_group/habitat_no_boat`,
          .data$`trip_group/Habitat_no_boat`
        ),
        1, 1
      ),
      `trip_group/habitat` = dplyr::coalesce(
        .data$`trip_group/habitat_boat`,
        .data$habitat_no_boat
      ),
      reason_no_activity = dplyr::coalesce(
        .data$reason_for_zero_boats,
        reason_no_fishing(
          .data$Tanba_sa_la_iha_ro_o_peskador_,
          .data$Seluk_hakerek_manualmente
        )
      ),
      catch_outcome = NA_character_
    ) %>%
    dplyr::select(-dplyr::any_of(c(
      "habitat_no_boat",
      "trip_group/Total_litru_mina_hir_e_ebe_gastu_ba_peska",
      "trip_group/fuel_used_L",
      "trip_group/habitat_boat",
      "trip_group/habitat_no_boat",
      "trip_group/Habitat_no_boat",
      "Tanba_sa_la_iha_ro_o_peskador_",
      "Seluk_hakerek_manualmente",
      "reason_for_zero_boats"
    )))
}

harmonise_v3 <- function(x) {
  x %>%
    dplyr::mutate(
      landing_site_name = dplyr::coalesce(
        !!!dplyr::select(., dplyr::contains("station"))
      ),
      # Two spellings of the same question. The old code coalesced the *names*
      # rather than the columns, so every v3 row carried the literal string
      # "group_conservation_trading/SE_FAAN_ITA_BO_OT_HAKARAK_FAA"; nothing
      # reads the column, so fixing it is free.
      `group_conservation_trading/trader` = dplyr::coalesce(
        .data$`group_conservation_trading/SE_FAAN_ITA_BO_OT_HAKARAK_FAA`,
        .data$`group_conservation_trading/trader`
      ),
      fuel_L = .data$`trip_group/fuel_used_L`,
      reason_no_activity = reason_no_fishing(
        .data$`group_info_general/Tanba_sa_la_iha_ro_o_peskador_`,
        .data$`group_info_general/Seluk_hakerek_manualmente`
      ),
      catch_use = .data$`group_conservation_trading/food_or_sale`
    ) %>%
    dplyr::select(-dplyr::any_of(c(
      "trip_group/fuel_used_L",
      "group_conservation_trading/SE_FAAN_ITA_BO_OT_HAKARAK_FAA",
      "group_conservation_trading/food_or_sale",
      "group_info_general/municipality",
      "group_info_general/Tanba_sa_la_iha_ro_o_peskador_",
      "form_completed"
    ))) %>%
    dplyr::select(-dplyr::contains("group_info_general/group_station")) %>%
    dplyr::rename(
      date = "group_info_general/date",
      Ita_koleta_dadus_husi_atividad =
        "group_info_general/Ita_koleta_dadus_husi_atividad",
      total_catch_value = "group_conservation_trading/total_catch_value"
    )
}

# "seluk__hakerek" means "other, written in"; the reason is then in the free
# text field.
reason_no_fishing <- function(reason, other) {
  dplyr::case_when(
    reason == "seluk__hakerek" ~ other,
    TRUE ~ reason
  )
}

# NA only when nothing at all was recorded, so a form that counted men but left
# women blank still reports a crew size.
sum_fishers <- function(...) {
  crew <- cbind(...)
  dplyr::if_else(
    rowSums(!is.na(crew)) == 0,
    NA_real_,
    rowSums(crew, na.rm = TRUE)
  )
}

#' Resolve raw catch codes into taxa
#'
#' The form records a catch as a number; the frame maps that number
#' (`survey_label`) to an FAO 3-alpha code and a scientific name. `0` is the
#' form's "no catch" sentinel and has no frame row.
#'
#' An unrecognised catch that still landed individuals or revenue becomes
#' `MZZ`, "marine fishes nei"; one with neither is a genuine no-catch trip.
#'
#' @param x A long catch table carrying `species` and `total_catch_value`.
#' @param labels Output of [survey_labels()].
#' @return `x` with `catch_taxon` and `scientific_name` resolved.
#' @keywords preprocessing
#' @noRd
resolve_catch_taxa <- function(x, labels) {
  x %>%
    dplyr::left_join(labels$taxa, by = c("species" = "survey_label")) %>%
    dplyr::mutate(
      catch_taxon = dplyr::if_else(
        !is.na(.data$species) & .data$species == "0", "0", .data$catch_taxon
      ),
      catch_taxon = dplyr::case_when(
        is.na(.data$catch_taxon) &
          .data$n_individuals > 0 |
          is.na(.data$catch_taxon) & !.data$total_catch_value == "0" ~ "MZZ",
        is.na(.data$catch_taxon) &
          is.na(.data$n_individuals) &
          is.na(.data$total_catch_value) |
          is.na(.data$catch_taxon) &
            is.na(.data$n_individuals) &
            .data$total_catch_value == "0" |
          is.na(.data$catch_taxon) &
            .data$n_individuals == 0 &
            is.na(.data$total_catch_value) |
          is.na(.data$catch_taxon) &
            .data$n_individuals == 0 &
            .data$total_catch_value == "0" ~ "0",
        TRUE ~ .data$catch_taxon
      )
    ) %>%
    dplyr::select(-"species")
}

#' Label lookup tables for preprocessing
#'
#' Taxa, gear, vessels and landing sites come from the PESKAS | FRAME assets
#' snapshot.
#'
#' @param conf The configuration file.
#' @return A named list of lookup tibbles keyed on the raw form value.
#' @keywords preprocessing
#' @export
survey_labels <- function(conf) {
  assets <- get_assets(conf)

  sites <- assets$sites %>%
    dplyr::transmute(
      landing_site_code = as.character(.data$site_code),
      # One frame site name carries an embedded newline. These are labels and
      # they end up in the API export, so squish rather than merely trim.
      landing_site = stringr::str_squish(.data$site),
      gaul_2_code = as.character(.data$gaul_2_code)
    ) %>%
    dplyr::distinct()

  geo <- assets$geo %>%
    dplyr::transmute(
      gaul_2_code = as.character(.data$gaul_2_code),
      .data$gaul_2_name,
      gaul_1_code = as.character(.data$gaul_1_code),
      .data$gaul_1_name
    ) %>%
    dplyr::distinct()

  list(
    taxa = assets$taxa %>%
      dplyr::transmute(
        survey_label = as.character(.data$survey_label),
        catch_taxon = as.character(.data$alpha3_code),
        .data$scientific_name
      ) %>%
      dplyr::distinct(),
    gear = assets$gear %>%
      dplyr::transmute(
        gear_code = as.character(.data$survey_label),
        gear = .data$standard_name
      ) %>%
      dplyr::distinct(),
    vessel = assets$vessels %>%
      dplyr::transmute(
        vessel_code = as.character(.data$survey_label),
        vessel_type = .data$standard_name
      ) %>%
      dplyr::distinct(),
    site = dplyr::left_join(sites, geo, by = "gaul_2_code"),
    habitat = habitat_labels()
  )
}

# The habitat code -> label lookup recorded on the survey form.
habitat_labels <- function() {
  tibble::tribble(
    ~habitat_code, ~habitat,
    "1", "Reef",
    "2", "FAD",
    "3", "Deep",
    "4", "Beach",
    "5", "Traditional FAD",
    "6", "Mangrove",
    "7", "Seagrass"
  )
}

#' Attach the standard gear, vessel, site and habitat labels
#'
#' @param x A long catch table carrying the `*_code` columns.
#' @param labels Output of [survey_labels()].
#' @return `x` with `gear`, `vessel_type`, `landing_site`, `gaul_*` and
#'   `habitat` added.
#' @keywords preprocessing
#' @noRd
resolve_survey_labels <- function(x, labels) {
  x %>%
    dplyr::left_join(labels$gear, by = "gear_code") %>%
    dplyr::left_join(labels$vessel, by = "vessel_code") %>%
    dplyr::left_join(labels$site, by = "landing_site_code") %>%
    dplyr::left_join(labels$habitat, by = "habitat_code")
}
