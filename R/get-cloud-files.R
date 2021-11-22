
get_validated_landings <- function(pars){
  cloud_object_name(
    prefix = paste0(pars$surveys$merged_landings$file_prefix, "_", "validated"),
    provider = pars$storage$google$key,
    options = pars$storage$google$options) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options) %>%
    readr::read_rds()
}

get_validated_pds_trips <- function(pars){
  cloud_object_name(
    prefix = paste0(pars$pds$trips$file_prefix, "_", "validated"),
    provider = pars$storage$google$key,
    options = pars$storage$google$options) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options) %>%
    readr::read_rds()
}


get_merged_trips <- function(pars, ...){
  cloud_object_name(
    prefix = paste0(pars$merged_trips$file_prefix),
    provider = pars$storage$google$key,
    options = pars$storage$google$options,
    ...) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options) %>%
    readr::read_rds()
}

get_public_files <- function(pars){
  c("trips", "catch", "aggregated") %>%
    rlang::set_names() %>%
    purrr::map(~ paste0(pars$export$file_prefix, "_", .)) %>%
    purrr::map(
      .f = cloud_object_name,
      extension = "rds",
      provider = pars$public_storage$google$key,
      options = pars$public_storage$google$options) %>%
    purrr::map(
      .f = download_cloud_file,
      provider = pars$public_storage$google$key,
      options = pars$public_storage$google$options) %>%
    purrr::map(readr::read_rds)
}

get_models <- function(pars){
  cloud_object_name(
    prefix = paste0(pars$models$file_prefix),
    provider = pars$storage$google$key,
    options = pars$storage$google$options) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options) %>%
    readr::read_rds()
}
