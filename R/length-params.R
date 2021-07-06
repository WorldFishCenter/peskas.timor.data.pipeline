#' Download metadata catch types
#'
#' This function download airtable tables containing metadata of catch types.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more
#'   details
#'
#' @return No output. This funcrion is used for it's side effects
#' @export
#'
get_catch_types <- function(log_threshold = logger::DEBUG) {
  pars <- read_config()

  metadata <- get_preprocessed_metadata(pars)

  catch_types <- metadata$catch_types
  fao_records <- metadata$fao_catch

  catches_tab <- dplyr::left_join(fao_records, catch_types, by = "interagency_code")

  spp <- catches_tab %>%
    dplyr::select(
      .data$interagency_code, .data$isscaap_taxocode.x, .data$name_scientific,
      .data$catch_family, .data$catch_name_en, .data$taxo_name_en
    )

  logger::log_info("Getting taxa ranks...")
  ranks <- taxize::tax_rank(spp$name_scientific, db = "gbif", rows = 1)


  ranks_tab <-
    ranks %>%
    purrr::map(~ stringr::str_to_title(.x)) %>%
    purrr::map(~ tidyr::as_tibble(.x)) %>%
    purrr::map_dfr(~ dplyr::mutate_all(.x, as.character)) %>%
    dplyr::rename(taxa_rank = .data$value) %>%
    dplyr::bind_cols(spp) %>%
    dplyr::mutate(name = gsub(" spp", "", .data$name_scientific)) %>%
    dplyr::mutate(name = gsub("\\s*\\([^\\)]+\\)", "", .data$name)) %>%
    dplyr::mutate(
      name = dplyr::case_when(
        name == "Thunnini" ~ "Tuna",
        name == "Selachimorpha" ~ "Shark",
        TRUE ~ .data$name
      ),
      taxa_rank = dplyr::case_when(
        name == "Tuna" ~ "comm_name",
        name == "Shark" ~ "comm_name",
        TRUE ~ .data$taxa_rank
      )
    ) %>%
    dplyr::filter(!.data$catch_name_en %in% c(
      "No catch", "Sea cucumber",
      "Other", "Unknown", "Lobster",
      "Herring"
    ))

  ranks_tab %>%
    dplyr::group_by(.data$interagency_code) %>%
    dplyr::filter(dplyr::row_number()==1)
}


#' Download species body-length parameters
#'
#' This function takes species, group of species or common names as arguments and
#' return a data frame containing body-length parameters associated to the
#' FishBase database.
#'
#' @param taxa The name of the species/groups from which retrieve lengths parameters.
#' @param rank The taxonomical ranking, can be `Species`, `Genus`, `Family`, or
#' `comm_name.` The latter is an abbreviation for the  common name of a species/group.
#' @param country_code The code of the country associated to FishBase database, default
#' code include Timor-Leste and Indonesia. Full list at
#' \url{https://www.fishbase.se/country/ListOfCountryCodes.php}.
#' @param get_one Logical, indicating wether the user wants to get length data
#' from the most recorded species.
#'
#' @return A data frame containing species length parameters
#' @export
#'
#' @examples
#' get_fish_length(
#'   taxa = "Sardine", rank = "comm_name",
#'   country_code = 626, get_one = FALSE
#' )
get_fish_length <- function(taxa,
                            rank = NULL,
                            country_code = NULL,
                            get_one = NULL) {
  take_one <- function(specs_length) {
    specs_length %>%
      dplyr::arrange(dplyr::desc(.data$CoeffDetermination)) %>%
      dplyr::slice(1)
  }

  if (rank == "comm_name") {
    sp_list <- rfishbase::common_to_sci(taxa) %>%
      magrittr::extract2("Species")
  } else {
    sp_list <- list(taxa) %>%
      rlang::set_names(rank) %>%
      do.call(rfishbase::species_list,.)
  }
  specs <- sp_list %>%
    rfishbase::country() %>%
    dplyr::filter(.data$C_Code %in% country_code) %>%
    magrittr::extract2("Species")
  specs_length <- rfishbase::length_weight(specs)

  if (isFALSE(get_one)) {
    return(specs_length)
  }
  return(take_one(specs_length))
}

#' Retrieve body-length parameters
#'
#' This function download body-length parameters from a table returned by
#' `get_catch_types` function.
#'
#' @param data A table returned from `get_catch_types`
#' @inheritParams get_fish_length
#'
#' @return A data frame containing species length parameters
#' @export
#'
#' @examples
#' \dontrun{
#' rank_tab <- get_catch_types()
#' retrieve_lengths(rank_tab)
#' }
#'
retrieve_lengths <- function(data, country_code = c(626, 360), get_one = FALSE) {
  res <- NULL
  for (i in unique(data$interagency_code)) {
    dat <- dplyr::filter(data,interagency_code %in% i)
    int_code <- unique(i)

    sp_len <-
      get_fish_length(
        taxa = dat$name,
        rank = dat$taxa_rank,
        country_code = country_code,
        get_one = get_one
      )

    sp_len <- dplyr::mutate(sp_len, interagency_code = rep(int_code, nrow(sp_len))) %>%
      dplyr::select(interagency_code, tidyselect::everything())

    res <- rbind(res, sp_len)
  }
  res
}
