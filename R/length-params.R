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

  catches_tab <- dplyr::left_join(fao_records, catch_types, by = "interagency_code")

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
        name == "Reptantia" ~ "Nephropidae",
        name == "Rajiformes" ~ "Myliobatiformes",
        TRUE ~ .data$name
      ),
      taxa_rank = dplyr::case_when(
        name == "Tuna" ~ "comm_name",
        name == "Shark" ~ "comm_name",
        name == "Nephropidae" ~ "Family",
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
      magrittr::extract2("Species")
  } else {
    sp_list <- list(taxa) %>%
      rlang::set_names(rank) %>%
      do.call(rfishbase::species_list, .)
  }

  if (taxa == "Abudefduf vaigiensis" | taxa == "Hyporhamphus quoyi") {
    specs_length <- rfishbase::length_weight(taxa)
    return(specs_length)
  }
  specs <- sp_list %>%
    rfishbase::country() %>%
    dplyr::filter(.data$C_Code %in% country_code) %>%
    magrittr::extract2("Species")
  specs_length <- rfishbase::length_weight(specs)

  specs_length
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
  pars <- read_config()

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
      dplyr::select(interagency_code, tidyselect::everything())

    rfish_tab <- rbind(rfish_tab, sp_len)
  }
  miss_groups <-
    dplyr::setdiff(
      get_preprocessed_metadata(pars)$catch_types$interagency_code,
      rfish_tab$interagency_code
    ) %>%
    dplyr::as_tibble() %>%
    dplyr::rename(interagency_code = .data$value) %>%
    na.omit(.data)

  rfish_tab <- dplyr::add_row(rfish_tab, miss_groups)
  rfish_tab
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


#' Join length-weights info to preprocessed landings
#'
#' The function integrate length-weight info from FIshBase databse to preprocessed
#' surveys data and convert catch labels according to the FAO nomenclature
#' (\url{http://www.fao.org/fishery/statistics/global-production/3/en}).
#'
#'
#' @param data The survey landings data frame
#'
#' @return A new landings data frame including length-weights info
#' @export
#'
join_weights <- function(data) {
  catch_codes <- get_preprocessed_metadata(pars)$catch_types %>%
    dplyr::transmute(
      species = as.character(.data$catch_number),
      catch_taxon = .data$interagency_code
    ) %>%
    dplyr::bind_rows(tibble::tibble(species = "0", catch_taxon = "0"))

  # download complete length-weight table
  rfish_tab <- get_rfish_table()

  # download manually filled length-weight info (groups not present in fishbase)
  manual_lw <-
    gsheet::gsheet2tbl("https://docs.google.com/spreadsheets/d/1vaS0jYz4NGJbHFXV8A6YjLegmRpIOOAbi3TL38_seps/edit#gid=0") %>%
    dplyr::select(-.data$DataRef)

  # filter by length type and exclude doubtful measurements (EsQ column)
  rfish_tab <- dplyr::bind_rows(rfish_tab, manual_lw) %>%
    dplyr::filter(.data$ interagency_code %in% "COZ" & is.na(.data$EsQ) & .data$Type %in% "ShL" |
                    .data$ interagency_code %in% c("IAX","OCZ") & is.na(.data$EsQ) &.data$Type %in% "ML"|
                    .data$ interagency_code %in% "CRA" & is.na(.data$EsQ) &.data$Type %in% "CW"|
                    .data$ interagency_code !=   c("COZ","IAX","OCZ","CRA") &
                    is.na(.data$EsQ) & .data$Type %in% c("TL","WD")|
                    .data$interagency_code %in% "RAB" & is.na(.data$EsQ)) %>%
    dplyr::select(
      .data$interagency_code, .data$LengthMin,
      .data$LengthMax, .data$a, .data$b
    ) %>%
    dplyr::rename(species = .data$interagency_code) %>%
    dplyr::group_by(.data$species) %>%
    dplyr::summarise_all(median, na.rm = TRUE)

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
    dplyr::left_join(rfish_tab) %>%
    dplyr::mutate(weight = (.data$a * .data$mean_length^.data$b) * .data$n_individuals) %>%
    tidyr::nest(length_individuals = c(
      .data$mean_length,
      .data$n_individuals,
      .data$n_individuals,
      .data$LengthMin,
      .data$LengthMax,
      .data$a,
      .data$b,
      .data$weight
    )) %>%
    tidyr::nest(species_group = c(
      .data$n,
      .data$species,
      .data$food_or_sale,
      .data$other_species_name,
      .data$photo,
      .data$length_individuals
    ))
}
