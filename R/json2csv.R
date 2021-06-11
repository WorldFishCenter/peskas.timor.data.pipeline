function(x) {
  url <- get_host_url("kobohr", "v2")
  pars <- read_config()

  metadata <- retrieve_survey_metadata(pars$surveys$landings$survey_id,
                                       token = pars$surveys$landings$token,
                                       api = "kobohr")

  d <- httr::GET(paste(url, "assets", metadata$id_string, "data", sep = "/"),
                 query = list(limit = 1,
                              start = 0),
                 config = httr::add_headers(Authorization = pars$surveys$landings$token))

  x <- d %>%
    httr::content() %>%
    magrittr::extract2("results") %>%
    magrittr::extract(1:10000) %>%
    jsonlite::toJSON() %>%
    jsonlite::fromJSON()

  b <- readr::read_csv("timor-landings-v2_raw__20210610045529_0a4c462__.csv", n_max = 1000)
  x <- d %>%
    httr::content() %>%
    magrittr::extract2("results") %>%
    magrittr::extract2(16)

  non_list_cols <- x %>%
    purrr::keep(~ class(.) != "list") %>%
    tibble::as_tibble()

  x %>%
    purrr::keep(~ class(.) == "list") %>%
    purrr::discard(~ length(.) == 0)

  list(x[[16]][c("_validation_status", "_id", "species_group","_geolocation")]) %>%
    purrr::map_dfr(flatten_row) %>% colnames()

  c <- x %>%
    purrr::map_dfr(flatten_row)

  y <- purrr::keep(x, ~ .$`_id` == 64080469)

  list(y[[1]]) %>%
    purrr::map_dfr(flatten_row)
  a <- jsonlite::read_json("timor-landings-v2_raw__20210610045529_0a4c462__.json.gz")

}


