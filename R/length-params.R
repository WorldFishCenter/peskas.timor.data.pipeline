#' Download metadata catch types
#'
#' This function downloads airtable tables containing catches metadata.
#'
#' @param pars The configuration file
#'
#' @return A dataframe containing taxonomic info and common names associated to
#'  species groups.
#' @export
#'
get_catch_types <- function(pars) {
  metadata <- get_preprocessed_metadata(pars)

  catch_types <- metadata$catch_types
  fao_records <- metadata$fao_catch

  catches_tab <- dplyr::left_join(catch_types, fao_records, by = "interagency_code") %>%
    dplyr::filter(!is.na(interagency_code))

  spp <- catches_tab %>%
    dplyr::select(
      .data$interagency_code, .data$isscaap_taxocode.x, .data$name_scientific,
      .data$catch_family, .data$catch_name_en, .data$taxo_name_en
    )

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
        name == "Rajiformes" ~ "Myliobatiformes",
        TRUE ~ .data$name
      ),
      taxa_rank = dplyr::case_when(
        name == "Tuna" ~ "comm_name",
        name == "Shark" ~ "comm_name",
        TRUE ~ .data$taxa_rank
      )
    )

  ranks_tab %>%
    dplyr::group_by(.data$interagency_code) %>%
    dplyr::filter(dplyr::row_number() == 1)
}


#' Download species body-length parameters
#'
#' This function takes species, group of species or common names as arguments and
#' returns a data frame containing weigth-length parameters from
#' FishBase database.
#'
#' @param taxa The name of the species/groups from which retrieve lengths parameters.
#' @param rank The taxonomical ranking, can be `Species`, `Genus`, `Family`, or
#' `comm_name.` The latter is an abbreviation for the  common name of a species/group.
#' @param country_code The code of the country associated to FishBase database, default
#' code include Timor-Leste and Indonesia. Full list at
#' \url{https://www.fishbase.se/country/ListOfCountryCodes.php}.
#'
#' @return A data frame containing species length parameters
#' @export
#'
#' @examples
#' get_fish_length(
#'   taxa = "Sardine", rank = "comm_name",
#'   country_code = 626
#' )
get_fish_length <- function(taxa,
                            rank = NULL,
                            country_code = NULL) {
  if (rank == "comm_name") {
    sp_list <- rfishbase::common_to_sci(taxa) %>%
      magrittr::extract2("Species") %>%
      unique()
  } else {
    sp_list <- list(taxa) %>%
      rlang::set_names(rank) %>%
      do.call(rfishbase::species_list, .) %>%
      unique()
  }

  country_table <- sp_list %>%
    rfishbase::country()

  specs <- country_table %>%
    dplyr::filter(.data$C_Code %in% country_code) %>%
    magrittr::extract2("Species") %>%
    unique()

  # If its not found in the country just get them all. THis is because it's
  # likely the species is present but there are no museum records in the
  # FishBase database
  if (length(specs) == 0) {
    specs <- country_table %>%
      magrittr::extract2("Species") %>%
      unique()
  }

  specs_weigth <- rfishbase::length_weight(specs)

  specs_length <- rfishbase::length_length(specs) %>%
    dplyr::filter(.data$Length1 %in% c("TL", "FL") &
      .data$Length2 %in% c("TL", "FL")) %>%
    dplyr::select(.data$Species, .data$Length1, .data$Length2,
      aL = .data$a, bL = .data$b
    )

  specs_lw <- dplyr::left_join(specs_weigth, specs_length, by = "Species")

  specs_lw
}

#' Retrieve weight-length parameters
#'
#' This function takes a table returned from `get_catch_types` and returns detailed
#' weight-length parameters from each species group.
#'
#' @param data A table returned from `get_catch_types`
#' @inheritParams get_fish_length
#'
#' @return A data frame containing species length parameters
#' @export
#'
#' @examples
#' \dontrun{
#' rank_tab <- get_catch_types(pars)
#' retrieve_lengths(rank_tab)
#' }
#'
retrieve_lengths <- function(data, country_code = c(626, 360)) {

  rfish_tab <- NULL

  for (i in unique(data$interagency_code)) {
    dat <- dplyr::filter(data, interagency_code %in% i)
    int_code <- unique(i)

    sp_len <-
      get_fish_length(
        taxa = dat$name,
        rank = dat$taxa_rank,
        country_code = country_code
      )

    sp_len <- dplyr::mutate(sp_len, interagency_code = rep(int_code, nrow(sp_len))) %>%
      dplyr::select(.data$interagency_code, tidyselect::everything())

    rfish_tab <- rbind(rfish_tab, sp_len)
  }
  miss_groups <-
    dplyr::setdiff(
      data$interagency_code,
      rfish_tab$interagency_code
    ) %>%
    dplyr::as_tibble() %>%
    dplyr::rename(interagency_code = .data$value) %>%
    na.omit(.data)

  rfish_tab <- dplyr::add_row(rfish_tab, miss_groups)
  rfish_tab
}

#' Join length-weights info to preprocessed landings
#'
#' The function integrate length-weight info from FIshBase databse to preprocessed
#' surveys data and convert catch labels according to the FAO nomenclature
#' (\url{http://www.fao.org/fishery/statistics/global-production/3/en}).
#'
#' The length types used to calculate weight in fish catches include total length
#' (TL) in survey version 1 and fork length (FL) in survey version 2 with the
#' exception of the group SRX (Myliobatiformes), which uses disk width (WD).
#' Weights of Non-fish groups are calculated according to carapace width (CW) in
#' crabs, mantel length (ML) in Cephalopoda and shell length (ShL) in Bivalvia,
#' carapace length (CL) in lobsters.
#'
#'
#' @param data The survey landings data frame
#'
#' @return A new landings data frame including length-weights info
#' @export
#'
join_weights <- function(data) {
  pars <- read_config()

  catch_codes <- get_preprocessed_metadata(pars)$catch_types %>%
    dplyr::transmute(
      species = as.character(.data$catch_number),
      catch_taxon = .data$interagency_code
    )

  rfish_tab <- get_preprocessed_metadata(pars)$morphometric_table

  # filter by length type and exclude doubtful measurements (EsQ column)
  rfish_tab <-
    rfish_tab %>%
    dplyr::filter(!.data$ interagency_code %in% c("COZ", "IAX", "OCZ", "CRA", "LOX") &
      is.na(.data$EsQ) & .data$Type %in% c("TL", "FL", "WD") |
      .data$ interagency_code %in% "COZ" & is.na(.data$EsQ) & .data$Type %in% "ShL" |
      .data$ interagency_code %in% c("IAX", "OCZ") & is.na(.data$EsQ) & .data$Type %in% "ML" |
      .data$ interagency_code %in% "CRA" & is.na(.data$EsQ) & .data$Type %in% "CW" |
      .data$ interagency_code %in% "LOX" & is.na(.data$EsQ) & .data$Type %in% "CL") %>%
    dplyr::rename(species = .data$interagency_code)

  # summarize weight and length parameters using median
  w_tab <-
    rfish_tab %>%
    dplyr::select(
      .data$species,
      .data$Type,
      .data$a,
      .data$b
    ) %>%
    dplyr::group_by(.data$species, .data$Type) %>%
    dplyr::summarise_all(median, na.rm = TRUE)

  l_tab <-
    rfish_tab %>%
    dplyr::select(.data$species, .data$aL, .data$bL, .data$Length1, .data$Length2) %>%
    dplyr::group_by(.data$species, .data$Length1, .data$Length2) %>%
    dplyr::summarise_all(median, na.rm = TRUE) %>%
    dplyr::rename(Type = .data$Length2)

  # Length1 = unknown length, Length2 = known length
  # join weight-length and length-length tables . The length
  # conversion formula is Length1 = aL + Length2 x bL

  wl_tab <- dplyr::left_join(w_tab, l_tab)

  data <-
    data %>%
    dplyr::mutate(
      species_group = purrr::map(
        .x = .data$species_group, .f = dplyr::left_join,
        catch_codes, by = c("species")
      ),
      species_group = purrr::map(
        .x = .data$species_group, .f = dplyr::select,
        -.data$species
      ),
      species_group = purrr::map(
        .x = .data$species_group, .f = dplyr::rename,
        species = .data$catch_taxon
      )
    ) %>%
    tidyr::unnest(.data$species_group, keep_empty = TRUE) %>%
    tidyr::unnest(.data$length_individuals, keep_empty = TRUE) %>%
    dplyr::left_join(wl_tab) %>%
    # Excluding FL and TL for weight calculation in legacy and recent landings
    # respectively. Keep group DRZ as it has FL=TL.
    dplyr::mutate(weight = dplyr::case_when(
      .data$survey_version == "v1" & !.data$Type == "FL" | .data$species == "DRZ" ~
      (.data$a * .data$mean_length^.data$b) * .data$n_individuals,
      .data$survey_version == "v2" & !.data$Type == "TL" | .data$species == "DRZ" ~
      (.data$a * .data$mean_length^.data$b) * .data$n_individuals
    )) %>%
    tidyr::nest(length_individuals = c(
      .data$mean_length,
      .data$n_individuals,
      .data$Type,
      .data$a,
      .data$b,
      .data$Length1,
      .data$aL,
      .data$bL,
      .data$weight
    )) %>%
    tidyr::nest(species_group = c(
      .data$n,
      .data$species,
      .data$food_or_sale,
      .data$other_species_name,
      .data$photo,
      .data$length_individuals
    )) %>%
    dplyr::mutate(catch_weight = purrr::map_dbl(.data$species_group, calc_total_weight))
}

# Helper function useful to calculate total catch weight associated to join_weights
calc_total_weight <- function(x) {
  x$length_individuals %>%
    purrr::map_dbl(~ sum(.$weight, na.rm = T)) %>%
    sum()
}


#' Ingest species weight-length info
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more
#'   details
#'
#' @return No output. This function is used for it's side effects
#' @export
#'
ingest_rfish_table <- function(log_threshold = logger::DEBUG) {
  pars <- read_config()

  rfish_tab <- get_catch_types(pars) %>%
    retrieve_lengths()

  rfish_table_filename <- paste(pars$metadata$rfishtable$file_prefix,
    sep = "_"
  ) %>%
    add_version(extension = "rds")
  readr::write_rds(
    x = rfish_tab,
    file = rfish_table_filename,
    compress = "gz"
  )
  logger::log_info("Uploading {rfish_table_filename} to cloud storage")
  upload_cloud_file(
    file = rfish_table_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}


# get weight-length table from google cloud
get_rfish_table <- function(log_threshold = logger::DEBUG) {
  pars <- read_config()

  rfish_rds <- cloud_object_name(
    prefix = paste(pars$metadata$rfishtable$file_prefix),
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$metadata$rfishtable$version,
    options = pars$storage$google$options,
    exact_match = TRUE
  )
  logger::log_info("Downloading {rfish_rds}...")
  download_cloud_file(
    name = rfish_rds,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
  readr::read_rds(file = rfish_rds)
}
