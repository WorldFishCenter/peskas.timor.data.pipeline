get_flags <- function(){

  logger::log_threshold(log_threshold)

  pars <- read_config()

  landings_csv <- cloud_object_name(prefix = pars$surveys$landings$file_prefix,
                                    provider = pars$storage$google$key,
                                    extension = "csv",
                                    version = pars$surveys$landings$version$preprocess,
                                    options = pars$storage$google$options)
}


get_previous_flags <- function() {

  googledrive::drive_auth(path = "auth/gcp-sa-peskas_ingestion-key.json")
  googlesheets4::gs4_auth(token = googledrive::drive_token())
  googlesheets4::read_sheet(
    ss = "1aquZSimR2okURO08q1lmoVUwNZvwwSwbpqblE4fMKk8",
    sheet = "validation") %>%
    dplyr::filter(!is.na(record_id))

}

function(){

  pars <- read_config()

  metadata_rds <- cloud_object_name(
    prefix = paste(pars$metadata$airtable$name, "preprocessed", sep = "_"),
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$metadata$version$preprocess,
    options = pars$storage$google$options)
  download_cloud_file(name = metadata_rds,
                      provider = pars$storage$google$key,
                      options = pars$storage$google$options)
  metadata <- readr::read_rds(file = metadata_rds)

  landings_rds <- cloud_object_name(
    prefix = pars$surveys$landings$file_prefix,
    provider = pars$storage$google$key,
    extension = "rds",
    version = pars$surveys$landings$version$preprocess,
    options = pars$storage$google$options)
  download_cloud_file(name = landings_rds,
                      provider = pars$storage$google$key,
                      options = pars$storage$google$options)
  landings <- readr::read_rds(file = landings_rds)

  flagging_records <- get_previous_flags()

  deployed_imeis <- get_deployed_imeis(metadata)

  landings$`trip_group/IMEI` %>%
    purrr::map(validate_this_imei, deployed_imeis) %>%
    purrr::map_dfr(tibble::as_tibble)


}

# Get a vector with imeis deployed in the field
get_deployed_imeis <- function(metadata){
  metadata$devices %>%
    dplyr::right_join(metadata$device_installs,
                      by = c("id" = "device_imei")) %>%
    dplyr::filter(!is.na(device_imei)) %>%
    magrittr::extract2("device_imei")
}





# Perform tests for a single imei and return the corrected value and the flag
validate_this_imei <- function(this_imei, valid_imeis){

  # If imei is NA there is nothing to validate
  if (is.na(this_imei)) {
    out <- list(imei = NA_character_, flag_id = NA_integer_)
    return(out)
  }

  # If the IMEI is negative it was probably a typo
  this_imei <- as.numeric(this_imei)
  if (this_imei < 0) this_imei <- this_imei * -1

  # Optimistically we need at least 5 digits to work with and that might be
  if (this_imei < 9999) {
    out <- list(imei = NA_character_, flag_id = 1)
    return(out)
  }

  # If a valid IMEI is found replace it
  imei_regex <- paste0(as.character(this_imei), "$")
  imei_matches <- stringr::str_detect(valid_imeis, imei_regex)
  n_matches <- sum(imei_matches)
  if (n_matches == 1) {
    list(imei = valid_imeis[imei_matches], flag_id = NA_integer_)
  } else if (n_matches > 1) {
    list(imei = NA_character_, flag_id = 2)
  } else if (n_matches == 0) {
    list(imei = NA_character_, flag_id = 3)
  }
}


function(){
  flagging_records %>%
    dplyr::filter(record_id == this_id)
}

function(record_id, value_to_test, validation_function, flagging_records){

}
