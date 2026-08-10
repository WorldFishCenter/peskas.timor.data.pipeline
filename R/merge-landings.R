#' Merge recent and legacy pre-processed landings
#'
#' Binds the pre-processed v2 and v3 landings with the frozen v1 snapshot and
#' reconciles the columns that differ between form versions.
#'
#' The merged file is then uploaded to the cloud. The parameters needed are:
#'
#' ```
#' surveys:
#'   landings_2:
#'     file_prefix:
#'     version:
#'       preprocess:
#'   landings_3:
#'     file_prefix:
#'     version:
#'       preprocess:
#'   landings:
#'     v1:
#'       frozen:
#'         file_prefix:
#'         version:
#'   merged_landings:
#'     file_prefix:
#'     version:
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a threshold for the logging infrastructure. See [logger::log_levels] for more details
#' @return No outputs. This function is used for it's side effects
#' @keywords workflow
#' @export
merge_landings <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  preprocessed_updated_landings <-
    coasts::cloud_object_name(
      prefix = paste(conf$surveys$landings_3$file_prefix,
        "preprocessed",
        sep = "_"
      ),
      provider = conf$storage$google$key,
      extension = "rds",
      version = conf$surveys$landings_3$version$preprocess,
      options = conf$storage$google$options
    )


  preprocessed_landings <-
    coasts::cloud_object_name(
      prefix = paste(conf$surveys$landings_2$file_prefix,
        "preprocessed",
        sep = "_"
      ),
      provider = conf$storage$google$key,
      extension = "rds",
      version = conf$surveys$landings_2$version$preprocess,
      options = conf$storage$google$options
    )

  logger::log_info("Retrieving preprocessed data")
  purrr::map(
    c(
      preprocessed_updated_landings,
      preprocessed_landings
    ),
    coasts::download_cloud_file,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  # adding a column "survey_version"
  prep_updated_landings <-
    readr::read_rds(preprocessed_updated_landings) %>%
    dplyr::mutate(survey_version = rep("v3", nrow(.)))

  prep_landings <-
    readr::read_rds(preprocessed_landings) %>%
    dplyr::mutate(survey_version = rep("v2", nrow(.)))

  # v1 is frozen: one snapshot of the last preprocessed output, with lengths
  # already converted from fork length to total length. See
  # `data-raw/freeze-landings-v1.R`.
  prep_legacy_landings <-
    download_versioned_rds(
      prefix = conf$surveys$landings$v1$frozen$file_prefix,
      provider = conf$storage$google$key,
      options = coasts::resolve_storage_opts(conf, "country"),
      version = conf$surveys$landings$v1$frozen$version
    ) %>%
    dplyr::mutate(survey_version = rep("v1", nrow(.)))

  merged_landings <-
    dplyr::bind_rows(prep_legacy_landings, prep_landings) %>%
    dplyr::bind_rows(prep_updated_landings) %>%
    merge_versions()

  merged_filename <- conf$surveys$merged_landings$file_prefix %>%
    add_version(extension = "rds")

  readr::write_rds(
    x = merged_landings,
    file = merged_filename,
    compress = "gz"
  )

  logger::log_info("Uploading {merged_filename} to cloud sorage")
  coasts::upload_cloud_file(
    file = merged_filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}

merge_versions <- function(x) {
  x %>%
    dplyr::mutate(
      fuel_L = dplyr::coalesce(
        .data$`trip_group/Total_litru_mina_hir_e_ebe_gastu_ba_peska`,
        .data$`trip_group/fuel_used_L`
      ),
      habitat_no_boat = dplyr::coalesce(
        .data$`trip_group/habitat_no_boat`,
        .data$`trip_group/Habitat_no_boat`
      ),
      habitat_no_boat = substr(.data$habitat_no_boat, 1, 1),
      `trip_group/habitat_boat` = dplyr::coalesce(
        .data$`trip_group/habitat_boat`,
        .data$habitat_no_boat
      ),
      reason_no_fishing = dplyr::case_when(
        .data$`Tanba_sa_la_iha_ro_o_peskador_` == "seluk__hakerek"
        ~ .data$`Seluk_hakerek_manualmente`, TRUE ~ .data$`Tanba_sa_la_iha_ro_o_peskador_`
      ),
      reason_no_activity = dplyr::coalesce(
        .data$reason_for_zero_boats,
        .data$reason_no_fishing
      )
    ) %>%
    dplyr::rename(`trip_group/habitat` = .data$`trip_group/habitat_boat`) %>%
    dplyr::select(-c(
      .data$habitat_no_boat,
      .data$`trip_group/Total_litru_mina_hir_e_ebe_gastu_ba_peska`,
      .data$`trip_group/fuel_used_L`,
      .data$`trip_group/habitat_no_boat`,
      .data$`trip_group/Habitat_no_boat`,
      .data$`Tanba_sa_la_iha_ro_o_peskador_`,
      .data$`Seluk_hakerek_manualmente`,
      .data$reason_for_zero_boats,
      .data$reason_no_fishing,
      # drop this column as it is all NA
      .data$`_bamboo_dataset_id`
    ))
}
