get_flags <- function(){
  googledrive::drive_auth(path = "auth/gcp-sa-peskas_ingestion-key.json")
  googlesheets4::gs4_auth(token = googledrive::drive_token())
  googlesheets4::read_sheet(
    ss = "1aquZSimR2okURO08q1lmoVUwNZvwwSwbpqblE4fMKk8",
    sheet = "validation") %>%
    dplyr::filter(!is.na(record_id))
}

function(){
  flags <- get_flags()
  pars <- read_config()

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
}


validate_imei <- function(imei, valid_imeis){

  # If the IMEI is negative it was probably a typo
  imei <- as.numeric(imei)
  if (imei < 0) imei <- imei * -1

  # Optimistically we need at least 5 digits to work with and that might be
  if (imei < 9999) {
    out <- list(imei = NA_character_, flag_id = 1)
    return(out)
  }

  # If a valid IMEI is found replace it
  imei_regex <- paste0(as.character(imei), "$")
  imei_matches <- stringr::str_detect(valid_imeis, imei_regex)
  n_matches <- sum(imei_matches)
  if (n_matches == 1) {
    list(imei = valid_imeis[imei_matches], flag = NA_integer_)
  } else if (n_matches > 1) {
    list(imei = NA_character_, flag_id = 2)
  } else if (n_matches == 0) {
    list(imei = NA_character_, flag_id = 3)
  }
}
