#' Clean SFF landings' catches table
#'
#' Legacy data (SFF landings) include catches information from different
#' versions of the project. This implies that the same information (same variables)
#' can show a different syntax and/or different column names. This function
#' converts and recodes the information associated with species catches into the
#' same format adopted for more recent landings.
#'
#' @param x A data frame containing raw legacy landings (SFF landings).
#'
#' @return A data frame containing species catches information in the same
#' syntax as recent landings.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' authentication_details <- readLines("location_of_json_file.json")
#' # obtain the latest version of all files corresponding to timor-landings-v1
#' legacy_data <-
#'   cloud_object_name(
#'     prefix = "timor-landings-v1",
#'     version = "latest",
#'     provider = "gcs",
#'     options = list(
#'       service_account_key = authentication_details,
#'       bucket = "my-bucket"
#'     )
#'   )
#'
#' legacy_raw <-
#'   readr::read_csv(file = legacy_data, col_types = readr::cols(.default = readr::col_character()))
#'
#' clean_catches(legacy_raw)
#' }
clean_catches <- function(x) {
  # split catches data based on version prefixes
  prefix_list <-
    purrr::map(
      purrr::set_names(c("Spp.", "Species.", "group_ob8uk86/Species/", "group_ob8uk86/Species.", "Species/")),
      ~ dplyr::select(x, tidyselect::starts_with(.x))
    )

  # standardize catches column names across splitted data
  for (i in 1:length(prefix_list)) {
    colnames(prefix_list[[i]]) <-
      textclean::mgsub(names(prefix_list[[i]]),
        pattern = c(
          "Choose_Species", "Species_001", "presu_per_", "nuumeru", "other_name",
          "number_of_individuals", "number_of_individuals_2", "number_of_individuals_3", "number_of_individuals_4",
          "number_of_individuals_5", "number_of_individuals_6", "number_of_individuals_7", "number_of_individuals_8",
          "number_of_individuals_9", "number_of_individuals_10", "number_of_individuals_11",
          "NUMERU_IKAN_LIU_60_cm", "IKAN_NARUK_LIU_60_cm_SUKAT_NO_",
          "take_a_picture_using_the_camera", "food_or_sale_", "group_ai0pp95",
          "Species", "Spp", "group_ob8uk86/Species.0.group_ob8uk86/Species", "group_ob8uk86/Species"
        ),
        replacement = c(
          "species", "species", "presu", "numeru", "other_species_name",
          "no_individuals_5_10", "no_individuals_10_15", "no_individuals_15_20", "no_individuals_20_25",
          "no_individuals_25_30", "no_individuals_30_35", "no_individuals_35_40", "no_individuals_40_45",
          "no_individuals_45_50", "no_individuals_50_55", "no_individuals_55_60",
          "no_individuals_over60", "fish_length_over60",
          "photo", "food_or_sale", "no_fish_by_length_group",
          "species_group", "species_group", "species_group.0.species_group", "species_group.0.species_group"
        ),
        fixed = TRUE, safe = TRUE
      )
  }

  # fix remaining syntax issues
  names(prefix_list$`Species/`) <- gsub(names(prefix_list$`Species/`),
    pattern = "species_group",
    replacement = "species_group.0.species_group"
  )


  fd1 <- purrr::map(
    purrr::set_names(c("foodsale", "food_or_sale")),
    ~ dplyr::select(prefix_list$Spp., tidyselect::contains(.x))
  )$foodsale
  names(fd1) <- gsub(names(fd1), pattern = "foodsale", replacement = "food_or_sale")
  fd2 <- prefix_list$Spp. %>% dplyr::select(-tidyselect::ends_with("foodsale"))

  prefix_list$Spp. <- dplyr::coalesce(fd1, fd2)

  # coalesce new catches columns and drop unused vars
  sp_table_cleaned <- dplyr::coalesce(!!!prefix_list) %>%
    dplyr::select(-c(
      tidyselect::contains("Fish_length", ignore.case = FALSE),
      tidyselect::contains("_per_Kg_hira"),
      tidyselect::contains("numeru_ikan"),
      tidyselect::ends_with("presu")
    )) %>%
    # recode "food or sale"
    dplyr::mutate(dplyr::across(
      tidyselect::ends_with("food_or_sale"),
      ~ dplyr::case_when(
        .x == "nain_rua" ~ "both",
        TRUE ~ .x
      )
    ))

  # remove groups' questions prefixes
  remove <- c("group_ob8uk86/", "group_st04m70/", "group_uw8jh69/")
  colnames(sp_table_cleaned) <- stringr::str_remove_all(
    names(sp_table_cleaned),
    paste(remove, collapse = "|")
  )

  # recode SSF landings species codes to peskas codes
  recoded_sp <-
    sp_table_cleaned %>%
    dplyr::select(tidyselect::ends_with("species")) %>%
    dplyr::mutate(row = dplyr::row_number()) %>%
    tidyr::pivot_longer(cols = -row) %>%
    dplyr::mutate(value = dplyr::case_when(
      value %in% c(93) ~ "1", value %in% c(86, 87, 211) ~ "2",
      value %in% c(2, 8, 46, 47, 48, 49, 50, 85, 103) ~ "3",
      value %in% c(5, 6, 7, 9, 91, 92, 94) ~ "4",
      value %in% c(41, 42, 43, 44, 45) ~ "5",
      value %in% c(88) ~ "6",
      value %in% c(90, 149) ~ "7",
      value %in% c(107, 109) ~ "8",
      value %in% c(1) ~ "9",
      value %in% c(31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 95, 96, 97, 98, 99, 100, 104) ~ "10",
      value %in% c(116, 117, 118) ~ "11",
      value %in% c(11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30) ~ "12",
      value %in% c(51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 143) ~ "13",
      value %in% c(119, 120, 121, 144) ~ "14",
      value %in% c(61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72) ~ "15",
      value %in% c(145, 146) ~ "16",
      value %in% c(89, 418) ~ "17",
      value %in% c(122, 123, 124, 125, 126, "mamila_matamouk") ~ "18",
      value %in% c(75) ~ "19",
      value %in% c(4, 101, 102) ~ "20",
      value %in% c(133, 134, 135, 136, 213) ~ "21",
      value %in% c(10) ~ "22",
      value %in% c(105, 106) ~ "23",
      value %in% c(153) ~ "24",
      value %in% c(417) ~ "25",
      value %in% c(416, 216) ~ "26",
      value %in% c(84) ~ "27",
      value %in% c(79, "bete_debedor") ~ "28",
      value %in% c(112, 113, 115) ~ "29",
      value %in% c(151) ~ "30",
      value %in% c(110, 111, 116, 150) ~ "31",
      value %in% c(80, 81) ~ "32",
      value %in% c(83, 214) ~ "33",
      value %in% c(140, 141, 142, 212) ~ "34",
      value %in% c(127, 128, 129, 130, 131, 132) ~ "35",
      value %in% c(217) ~ "36",
      value %in% c(215) ~ "37",
      value %in% c(219) ~ "38",
      value %in% c(148) ~ "39",
      value %in% c(152) ~ "41",
      value %in% c(74, 76) ~ "42",
      value %in% c(114) ~ "43",
      value %in% c(203) ~ "44",
      value %in% c(420) ~ "45",
      value %in% c(3) ~ "46",
      value %in% c(147) ~ "47",
      value %in% c(73) ~ "48",
      value %in% c(77, 78, 82) ~ "49",
      value %in% c(218) ~ "50",
      value %in% c("budu_tasi") ~ "58",
      value %in% c(
        139, 209, 300, 999, "babuti", "ikan_kulu", "kabosu", "kalepa_makerek",
        "lea", "lepulai", "pakulai", "teura_marubi"
      ) ~ "300",
      TRUE ~ value
    )) %>%
    tidyr::pivot_wider(names_from = "name", values_from = "value") %>%
    dplyr::select(-row)

  dplyr::bind_cols(
    sp_table_cleaned %>% dplyr::select(-tidyselect::ends_with("species")),
    recoded_sp
  )
}


#' Coalesce vectors
#'
#' This function coalesces vectors (see [dplyr::coalesce]) from a selected data
#' frame into a new single vector. The vectors to coalesce are collected into a
#' list (argument `to_coal`) and the output vector takes the name of the list'
#' element in which it is collected.
#'
#'
#' @param data Data frame from which select vectors to coalesce.
#'
#' @param to_coal List of vectors to coalesce. Coalesced vectors assume the name
#' of the element of the list in which they are collected.
#'
#' @param return_dat Logical argument indicating whether to return the whole
#' data frame plus the new coalesced vectors, or a new data frame constituted
#' only by the coalesced vectors.
#'
#' @return A data frame containing the coalesced vector(s)
#' @export
#'
#' @examples
#' \dontrun{
#' authentication_details <- readLines("location_of_json_file.json")
#' # obtain the latest version of all files corresponding to timor-landings-v1
#' legacy_data <-
#'   cloud_object_name(
#'     prefix = "timor-landings-v1",
#'     version = "latest",
#'     provider = "gcs",
#'     options = list(
#'       service_account_key = authentication_details,
#'       bucket = "my-bucket"
#'     )
#'   )
#'
#' legacy_raw <-
#'   readr::read_csv(
#'     file = legacy_data, col_types =
#'       readr::cols(.default = readr::col_character())
#'   )
#'
#' to_coal <- list(
#'   "date" = legacy_raw %>% dplyr::select("Data", "Date"),
#'   "trip_group/gear_type" = legacy_raw %>%
#'     dplyr::select(tidyselect::contains("gear")),
#'   "trip_group/habitat_boat" = legacy_raw %>%
#'     dplyr::select(tidyselect::contains("habitat"))
#' )
#'
#' coalist(legacy_raw, to_coal, return_dat = FALSE)
#' }
#'
coalist <- function(data, to_coal, return_dat = FALSE) {
  vecs <- NULL
  for (i in 1:length(to_coal)) {
    vec <- dplyr::coalesce(!!!to_coal[[i]])
    vecs <- cbind(vecs, vec)
  }

  colnames(vecs) <- names(to_coal)

  if (return_dat == TRUE) {
    cbind(data, vecs) %>%
      dplyr::select(-c(unlist(lapply(to_coal, colnames), use.names = FALSE))) %>%
      tidyr::as_tibble()
  } else {
    tidyr::as_tibble(vecs)
  }
}


#' Clean SFF landings data
#'
#' Legacy landings data frame has a different structure and uses a different
#' syntax from recent landings. This function restructures legacy raw data to
#' the same format and syntax as recent landings.
#'
#' @param x Data frame containing raw data from legacy landings (SSF landings).
#'
#' @return Data frame containing legacy landings data in the same syntax as
#' recent landings.
#' @export
#'
#' @examples
#' \dontrun{
#' authentication_details <- readLines("location_of_json_file.json")
#' # obtain the latest version of all files corresponding to timor-landings-v1
#' legacy_data <-
#'   cloud_object_name(
#'     prefix = "timor-landings-v1",
#'     version = "latest",
#'     provider = "gcs",
#'     options = list(
#'       service_account_key = authentication_details,
#'       bucket = "my-bucket"
#'     )
#'   )
#'
#' legacy_raw <-
#'   readr::read_csv(file = legacy_data, col_types = readr::cols(.default = readr::col_character()))
#'
#' clean_legacy_landings(legacy_raw)
#' }
clean_legacy_landings <- function(x) {
  # select common vars to SFF and peskas landings
  common_cols_table <- x %>% dplyr::select(
    c(
      "__version__", "_bamboo_dataset_id", "_id",
      "_status", "_submission_time", "_submitted_by", "_uuid", "_validation_status.by_whom",
      "_validation_status.color", "_validation_status.label", "_validation_status.timestamp",
      "_validation_status.uid", "_version_", "_version__001", "_xform_id_string", "deviceid",
      "end", "formhub/uuid", "meta/instanceID", "start", "today"
    )
  )

  # select and rename some SFF vars
  renamed_cols_table <- x %>%
    dplyr::select(c(
      "Ita_kolecta_dadus_husi_activid", "Site_name", "No_boats",
      "TOTAL_folin_ikan_hamutuk", "Ema_hira_halo_actividade_peska",
      "Tanba_sa_la_iha_atividade_peska_ohin", "group_ob8uk86/TOTAL_ORAS_VIAGEM_PESKA",
      "_geolocation1", "_geolocation2"
    )) %>%
    dplyr::rename(
      "_geolocation.0" = "_geolocation1",
      "_geolocation.1" = "_geolocation2",
      "Ita_koleta_dadus_husi_atividad" = "Ita_kolecta_dadus_husi_activid",
      "landing_site_name" = "Site_name",
      "no_boats" = "No_boats",
      "total_catch_value" = "TOTAL_folin_ikan_hamutuk",
      "how_many_gleaners_today" = "Ema_hira_halo_actividade_peska",
      "reason_for_zero_boats" = "Tanba_sa_la_iha_atividade_peska_ohin",
      "trip_group/duration" = "group_ob8uk86/TOTAL_ORAS_VIAGEM_PESKA"
    )

  # list of vectors to coalesce
  to_coal <- list(
    "date" = x %>% dplyr::select("Data", "Date"),
    "trip_group/gear_type" = x %>% dplyr::select(tidyselect::contains("gear")),
    "trip_group/habitat_boat" = x %>% dplyr::select(tidyselect::contains("habitat")),
    "happiness_rating" = x %>% dplyr::select(tidyselect::contains("happiness")),
    "trip_group/no_fishers/no_women_fishers" = x %>% dplyr::select(tidyselect::contains("women")),
    "trip_group/no_fishers/no_child_fishers" = x %>% dplyr::select(tidyselect::contains("child")),
    "trip_group/no_fishers/no_men_fishers" = x %>% dplyr::select(tidyselect::contains("mane_men")),
    "trip_group/mesh_size" = x %>% dplyr::select(tidyselect::contains("mesh_size", ignore.case = FALSE)),
    "trip_group/mesh_size_other" = x %>% dplyr::select(tidyselect::contains("Mesh_size", ignore.case = FALSE)),
    "trip_group/boat_type" = x %>% dplyr::select(tidyselect::contains("transport", ignore.case = FALSE)),
    "trip_group/boat_owner_name" = x %>% dplyr::select(
      tidyselect::contains("fisher"),
      -c(
        "group_ob8uk86/fisher_name",
        "group_ob8uk86/peskador_naran_sa_fisher_name_",
        "peskador_naran_sa_fisher_name_"
      )
    )
  )

  # select coalesced vectors
  coalesced_table <- coalist(x, to_coal, return_dat = FALSE)
  # clean and recode catches table
  catches_table <- clean_catches(x)
  # select attachments table
  attachments_table <- x %>% dplyr::select(tidyselect::contains("attachment"))

  # bind and recode the new vars
  dplyr::bind_cols(
    common_cols_table,
    renamed_cols_table,
    coalesced_table,
    catches_table,
    attachments_table
  ) %>%
    dplyr::mutate(`trip_group/mesh_size` = dplyr::case_when(
      `trip_group/mesh_size` == "AUTOMATIC" ~ "4",
      TRUE ~ `trip_group/mesh_size`
    )) %>%
    dplyr::mutate(`trip_group/gear_type` = dplyr::case_when(
      `trip_group/gear_type` == "1" ~ "GN",
      `trip_group/gear_type` == "2" ~ "HL",
      `trip_group/gear_type` == "3" ~ "LL",
      `trip_group/gear_type` == "4" ~ "SG",
      `trip_group/gear_type` == "5" ~ "CN",
      `trip_group/gear_type` == "6" ~ "MC",
      `trip_group/gear_type` == "7" ~ "BS",
      `trip_group/gear_type` == "8" ~ "SN",
      TRUE ~ `trip_group/gear_type`
    )) %>%
    dplyr::mutate(`trip_group/habitat_boat` = dplyr::case_when(
      `trip_group/habitat_boat` == "rompun" ~ "2",
      `trip_group/habitat_boat` == "deep" ~ "3",
      TRUE ~ `trip_group/habitat_boat`
    )) %>%
    dplyr::mutate(landing_site_name = dplyr::case_when(
      landing_site_name == "LOR" ~ NA_character_,
      landing_site_name == "COM" ~ "7",
      TRUE ~ landing_site_name
    )) %>%
    dplyr::mutate(`trip_group/boat_type` = dplyr::case_when(
      `trip_group/boat_type` == "3" ~ NA_character_,
      TRUE ~ `trip_group/boat_type`
    ))
}



#' Clean Updated (Peskas 2) landings data
#'
#' New Peskas 2 survey landings include some new questions and some new edits.
#' This function facilitates the integration of the new survey data with the
#' legacy ones.
#'
#' @param x Data frame containing raw data from updated landings (Peskas 2).
#'
#' @return Data frame containing updated landings data in the same syntax as
#' old landings.
#' @export
#'
#' @examples
#' \dontrun{
#' authentication_details <- readLines("location_of_json_file.json")
#' # obtain the latest version of all files corresponding to timor-landings-v1
#' peskas2_data <-
#'   cloud_object_name(
#'     prefix = "timor-landings-v3",
#'     version = "latest",
#'     provider = "gcs",
#'     options = list(
#'       service_account_key = authentication_details,
#'       bucket = "my-bucket"
#'     )
#'   )
#'
#' peskas2_raw <-
#'   readr::read_csv(file = peskas2_data, col_types = readr::cols(.default = readr::col_character()))
#'
#' clean_updated_landings(legacy_raw)
#' }
clean_updated_landings <- function(x) {
  x %>%
    dplyr::mutate(
      landing_site_name = dplyr::coalesce
      (!!!dplyr::select(., dplyr::contains
        ("station"))),
      `_id` = as.character(.data$`_id`)
    ) %>%
    dplyr::select(-c(
      dplyr::contains("group_info_general/group_station"),
      "group_info_general/municipality",
      "form_completed"
    )) %>%
    dplyr::rename(
      date = "group_info_general/date",
      Ita_koleta_dadus_husi_atividad = "group_info_general/Ita_koleta_dadus_husi_atividad",
      Tanba_sa_la_iha_ro_o_peskador_ = "group_info_general/Tanba_sa_la_iha_ro_o_peskador_",
      total_catch_value = "group_conservation_trading/total_catch_value",
      `trip_group/habitat_boat` = "trip_group/habitat",
      `group_conservation_trading/trader` = "group_conservation_trading/SE_FAAN_ITA_BO_OT_HAKARAK_FAA"
    )
}
