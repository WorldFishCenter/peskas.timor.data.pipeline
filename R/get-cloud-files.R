
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


get_merged_trips <- function(pars){
  cloud_object_name(
    prefix = paste0(pars$merged_trips$file_prefix),
    provider = pars$storage$google$key,
    options = pars$storage$google$options) %>%
    download_cloud_file(
      provider = pars$storage$google$key,
      options = pars$storage$google$options) %>%
    readr::read_rds()
}
