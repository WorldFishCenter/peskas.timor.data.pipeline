
test_that("host URLs are resolved correctly", {

  # All providers should have V1 available
  c("kobo", "kobohr", "unhcr", "ona") %>%
    purrr::map(get_host_url, version = "v1") %>%
    purrr::map_lgl(RCurl::url.exists) %>%
    all() %>%
    expect_true()

  # In addition Kobo should have v2 of the API also implemented
  c("kobo", "kobohr") %>%
    purrr::map(get_host_url, version = "v2") %>%
    purrr::map_lgl(RCurl::url.exists) %>%
    all() %>%
    expect_true()

})
