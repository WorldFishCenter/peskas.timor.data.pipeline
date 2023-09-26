#' Add weight of species to merged landings
#'
#' Downloads merged landings and calculates the weight of the catch.
#'
#' The  file is then uploaded to the cloud. The name is the same as for merged
#' landings but with "_weight" at the end. The parameters needed are:
#'
#' ```
#' surveys:
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
calculate_weights <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  merged_landings <- get_merged_landings(pars)
  metadata <- get_preprocessed_metadata(pars)
  morphometric_tables <- get_morphometric_tables(pars, metadata$morphometric_table)
  nutrients_table <- get_nutrients_table(pars) %>%
    dplyr::rename(species = .data$interagency_code)

  landings_with_weight <- join_weights(
    merged_landings,
    metadata,
    morphometric_tables,
    nutrients_table
  )

  landings_with_weight_filename <- paste(pars$surveys$merged_landings$file_prefix, "weight", sep = "_") %>%
    add_version(extension = "rds")
  readr::write_rds(
    x = landings_with_weight,
    file = landings_with_weight_filename,
    compress = "gz"
  )

  logger::log_info("Uploading {landings_with_weight_filename} to cloud sorage")
  upload_cloud_file(
    file = landings_with_weight_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}


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
    dplyr::filter(!is.na(.data$interagency_code))

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
        name == "Hyporhamphus quoyi" ~ "Garfish",
        TRUE ~ .data$name
      ),
      taxa_rank = dplyr::case_when(
        name == "Tuna" ~ "comm_name",
        name == "Shark" ~ "comm_name",
        name == "Garfish" ~ "comm_name",
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
#' \dontrun{
#' get_fish_length(
#'   taxa = "Sardine", rank = "comm_name",
#'   country_code = 626
#' )
#' }
get_fish_length <- function(taxa,
                            rank = NULL,
                            country_code = NULL) {
  if (rank == "comm_name") {
    sp_list <- rfishbase::common_to_sci(taxa) %>%
      magrittr::extract2("Species") %>%
      unique()
  } else if (rank == "Class") {
    sp_list <- rfishbase::species_list(Class = taxa) %>% unique()
  } else if (rank == "Order") {
    sp_list <- rfishbase::species_list(Order = taxa) %>% unique()
  } else if (rank == "Family") {
    sp_list <- rfishbase::species_list(Family = taxa) %>% unique()
  } else if (rank == "Genus") {
    sp_list <- rfishbase::species_list(Genus = taxa) %>% unique()
  } else if (rank == "Species") {
    sp_list <- rfishbase::species_list(Species = taxa) %>% unique()
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
#' @importFrom stats na.omit
#'
#' @examples
#' \dontrun{
#' rank_tab <- get_catch_types(pars)
#' retrieve_lengths(rank_tab)
#' }
#'
retrieve_lengths <- function(data, country_code) {
  rfish_tab <- NULL

  for (i in unique(data$interagency_code)) {
    dat <- dplyr::filter(data, .data$interagency_code %in% i)
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

#' Join length-weights and nutritional parameters info to preprocessed landings
#'
#' The function integrate nutritional info from the `get_nutrients_table` function
#' and length-weight info from FIshBase databse to preprocessed surveys data and
#' convert catch  labels according to the FAO nomenclature
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
#' @param metadata Metadata tables
#' @param rfish_tab Table with length weight parameters
#' @param nutrients_table Table with nutritional parameters
#'
#' @return A new landings data frame including length-weights info
#' @export
#'
join_weights <- function(data, metadata, rfish_tab, nutrients_table) {
  catch_codes <- metadata$catch_types %>%
    dplyr::transmute(
      species = as.character(.data$catch_number),
      catch_taxon = .data$interagency_code,
      length_type = .data$length_type
    ) %>%
    dplyr::mutate(catch_taxon = dplyr::if_else(.data$species == "0", "0", .data$catch_taxon))

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
    # fix conditions for "no catch" and "other" labels
    dplyr::mutate(species = dplyr::case_when(
      is.na(.data$species) & .data$n_individuals > 0 |
        is.na(.data$species) & !.data$total_catch_value == "0" ~ "MZZ",
      is.na(.data$species) & is.na(.data$n_individuals) & is.na(.data$total_catch_value) |
        is.na(.data$species) & is.na(.data$n_individuals) & .data$total_catch_value == "0" |
        is.na(.data$species) & .data$n_individuals == 0 & is.na(.data$total_catch_value) |
        is.na(.data$species) & .data$n_individuals == 0 & .data$total_catch_value == "0" ~ "0",
      TRUE ~ .data$species
    )) %>%
    # Excluding FL and TL for weight calculation in legacy and recent landings
    # respectively. Keep group DRZ as it has FL=TL.
    dplyr::mutate(length_type = dplyr::case_when(
      !is.na(length_type) ~ length_type,
      .data$survey_version == "v1" ~ "FL",
      .data$survey_version %in%  c("v2", "v3") ~ "TL"
    )) %>%
    dplyr::mutate(length_type = dplyr::case_when(
      .data$species %in% c("OCZ", "SLV", "IAX") ~ "TL",
      .data$species == "MOO" ~ "TL",
      TRUE ~ .data$length_type
    )) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(weight = estimate_weight(
      length = .data$mean_length, .data$length_type,
      code = .data$species, .data$n_individuals,
      rfish_tab$length_weight,
      rfish_tab$length_length
    )) %>%
    dplyr::ungroup() %>%
    dplyr::left_join(nutrients_table, by = "species") %>%
    dplyr::mutate(
      weight = abs(.data$weight),
      dplyr::across(
        c(.data$Selenium_mu:.data$Vitamin_A_mu),
        ~ .x * .data$weight
      )
    ) %>%
    tidyr::nest(length_individuals = c(
      .data$mean_length,
      .data$n_individuals,
      .data$weight,
      tidyselect::ends_with("_mu")
    )) %>%
    tidyr::nest(species_group = c(
      .data$n,
      .data$species,
      .data$food_or_sale,
      .data$other_species_name,
      .data$photo,
      .data$length_individuals,
      .data$length_type
    ))
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
    retrieve_lengths(country_code = pars$metadata$rfishtable$country_codes)

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
get_rfish_table <- function(pars) {
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

#
get_morphometric_tables <- function(pars, manual_table) {
  rfish_table <- get_rfish_table(pars) %>%
    dplyr::mutate(
      DataRef = as.character(.data$DataRef),
      LengthMin = as.double(.data$LengthMin),
      aL = as.double(.data$aL)
    )

  # merge the two tables
  rfish_tab <- dplyr::bind_rows(rfish_table, manual_table)

  lw <- rfish_tab %>%
    # There are extra coefficients in the aTL column that we should use
    dplyr::mutate(Type = "TL", a = .data$aTL) %>%
    dplyr::bind_rows(rfish_tab) %>%
    # There are some that have "No" low quality which means is OK?
    dplyr::filter(!is.na(.data$a), !isTRUE(tolower(.data$EsQ) == "yes")) %>%
    dplyr::select(.data$interagency_code, .data$Species, .data$LengthMin, .data$LengthMax, .data$Type, .data$a, .data$b) %>%
    dplyr::distinct()

  ll <- rfish_tab %>%
    # Relationship is reciprocal and we could use that information too
    dplyr::mutate(
      l1 = .data$Length1, l2 = .data$Length2, a1 = .data$aL, b1 = .data$bL,
      Length1 = .data$l2, Length2 = .data$l1, aL = .data$a1 / .data$b1 * (-1), bL = 1 / .data$b1
    ) %>%
    dplyr::bind_rows(rfish_tab) %>%
    dplyr::select(.data$interagency_code, .data$Species, .data$Length1, .data$Length2, .data$aL, .data$bL) %>%
    dplyr::filter(!is.na(.data$aL)) %>%
    dplyr::distinct()

  list(
    length_weight = lw,
    length_length = ll
  )
}


estimate_weight <- function(length, length_type, code, n_individuals, lw, ll) {
  if (is.na(length) | is.na(length_type) | is.na(code) | is.na(n_individuals) | code == "0") {
    return(NA)
  }

  if (n_individuals == 0) {
    return(0)
  }

  this_lw <- lw %>%
    dplyr::filter(.data$interagency_code == code)

  # Transform length to other types if relevant
  this_ll <- ll %>%
    dplyr::filter(.data$interagency_code == code, .data$Length2 == length_type) %>%
    dplyr::mutate(
      length = .data$aL + length * .data$bL,
      Type = .data$Length1
    )

  this_length <-
    dplyr::tibble(
      interagency_code = code,
      # We have to do it for each species because we don't know what is the
      # actual species
      Species = unique(this_lw$Species),
      length = length,
      Type = length_type
    ) %>%
    # Augment the info with transformations
    dplyr::bind_rows(this_ll)

  w <- dplyr::full_join(this_lw, this_length,
    by = c("interagency_code", "Species", "Type")
  ) %>%
    dplyr::mutate(
      weight = .data$a * .data$length^.data$b,
      quantile_coeff = dplyr::case_when(
        unique(.data$interagency_code) == "FLY" ~ 0.75,
        unique(.data$interagency_code) == "CGX" ~ 0.75,
        unique(.data$interagency_code) %in% c("EMP", "CLP") ~ 0.5,
        TRUE ~ 0.75
      )
    ) %>%
    dplyr::filter(!is.na(.data$weight))

  stats::quantile(w$weight, stats::median(w$quantile_coeff)) * n_individuals
}
