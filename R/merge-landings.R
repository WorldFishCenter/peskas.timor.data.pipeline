#' Merge recent and legacy pre-processed landings
#'
#' Downloads pre-processed versions of recent and legacy landings from cloud and
#' merge in a rds-format file using the function [dplyr::full_join].
#'
#' The merged file is then uploaded to the cloud. The parameters needed are:
#'
#' ```
#' surveys:
#'   landings:
#'     file_prefix:
#'     version:
#'       preprocess:
#'   landings_legacy:
#'     file_prefix:
#'     version:
#'       preprocess:
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
#' @param log_threshold
#' @inheritParams preprocess_legacy_landings
#' @return No outputs. This function is used for it's side effects
#' @keywords workflow
#' @export
merge_landings <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  preprocessed_updated_landings <-
    cloud_object_name(
      prefix = paste(pars$surveys$landings_3$file_prefix,
        "preprocessed",
        sep = "_"
      ),
      provider = pars$storage$google$key,
      extension = "rds",
      version = pars$surveys$landings_3$version$preprocess,
      options = pars$storage$google$options
    )


  preprocessed_landings <-
    cloud_object_name(
      prefix = paste(pars$surveys$landings_2$file_prefix,
        "preprocessed",
        sep = "_"
      ),
      provider = pars$storage$google$key,
      extension = "rds",
      version = pars$surveys$landings_2$version$preprocess,
      options = pars$storage$google$options
    )

  preprocessed_legacy_landings <-
    cloud_object_name(
      prefix = paste(pars$surveys$landings_1$file_prefix,
        "preprocessed",
        sep = "_"
      ),
      provider = pars$storage$google$key,
      extension = "rds",
      version = pars$surveys$landings_1$version$preprocess,
      options = pars$storage$google$options
    )

  logger::log_info("Retrieving preprocessed data")
  purrr::map(
    c(
      preprocessed_updated_landings,
      preprocessed_landings,
      preprocessed_legacy_landings
    ),
    download_cloud_file,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  # adding a column "survey_version"
  prep_updated_landings <-
    readr::read_rds(preprocessed_updated_landings) %>%
    dplyr::mutate(survey_version = rep("v3", nrow(.)))

  prep_landings <-
    readr::read_rds(preprocessed_landings) %>%
    dplyr::mutate(survey_version = rep("v2", nrow(.)))

  prep_legacy_landings <-
    readr::read_rds(preprocessed_legacy_landings) %>%
    dplyr::mutate(survey_version = rep("v1", nrow(.)))

  merged_landings <-
    dplyr::bind_rows(prep_legacy_landings, prep_landings) %>%
    dplyr::bind_rows(prep_updated_landings) %>%
    merge_versions()

  merged_filename <- pars$surveys$merged_landings$file_prefix %>%
    add_version(extension = "rds")

  readr::write_rds(
    x = merged_landings,
    file = merged_filename,
    compress = "gz"
  )

  logger::log_info("Uploading {merged_filename} to cloud sorage")
  upload_cloud_file(
    file = merged_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
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
