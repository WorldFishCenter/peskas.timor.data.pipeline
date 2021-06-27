length_params_sp <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)

  pars <- read_config()

  fao_records <- air_get_records(table="fao_catch",
                                 base_id=pars$metadata$airtable$base_id,
                                 api_key=readLines("auth/airtable-key")) %>%
    air_records_to_tibble()

  # get sp ranked table by isscaap_taxocode
  timor_sp <- dplyr::filter(fao_records, !grepl("X",isscaap_taxocode) &
                              !is.na(isscaap_code))

  # retrieve length params
  params_sp <-
    rfishbase::length_length(timor_sp$name_scientific) %>%
    dplyr::group_by(Species) %>%
    dplyr::arrange(desc(Number)) %>% # arrange by number of records and take max
    dplyr::slice(1)

  params_sp
}


length_params_spp <- function(log_threshold = logger::DEBUG){

  logger::log_threshold(log_threshold)

  pars <- read_config()

  fao_records <- air_get_records(table="fao_catch",
                                 base_id=pars$metadata$airtable$base_id,
                                 api_key=readLines("auth/airtable-key")) %>%
    air_records_to_tibble()

  # get spp ranked table by isscaap_taxocode
  spp <- dplyr::filter(fao_records, grepl("X",isscaap_taxocode) &
                               !is.na(isscaap_code))

  spp$name_scientific <- stringr::word(spp$name_scientific, 1)

  # Use taxize::tax_rank to get taxonomical rank
  ranks <- taxize::tax_rank(spp$name_scientific,db = "gbif",rows=1)

  ranks_tab <-
    ranks %>%
    purrr::map(~stringr::str_to_title(.x)) %>%
    purrr::map(~tidyr::as_tibble(.x)) %>%
    purrr::map_dfr(~dplyr::mutate_all(.x,as.character)) %>%
    dplyr::rename(taxa_rank=value) %>%
    dplyr::bind_cols(spp) %>%
    dplyr::select(taxa_rank,name_scientific)

  # Example with one record: get species from timor using country code
  timor_spp <-
    rfishbase::country(rfishbase::species_list(Genus="Upeneus")) %>%
    dplyr::filter(C_Code==626) %>%
    magrittr::extract2("Species")

  params_spp <-
    rfishbase::length_length(timor_spp) %>%
    dplyr::arrange(desc(Number)) %>% # arrange by number of records and take max
    dplyr::slice(1)

  params_spp
}
