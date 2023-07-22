#' Nest attachment columns
#'
#' Nests attachment columns obtained when reading structured data from the kobo
#' landings survey
#'
#' One of the disadvantages of using structured survey data is that the tables
#' can become very wide (many columns). This happens when question groups or
#' other fields can be recorded multiple times. For example in the landings
#' survey, for each species captured, about 17 questions are recorded. There is
#' no limit to the number of species that can be recorded in the trip. If, for
#' example a survey records seven species we will have over a hundred columns in
#' the data corresponding to species information.
#'
#' To improve that situation an avoid using multiple tables we use **nested data
#' frames** (see [tidyr::nest]). In nested data frames columns can be lists and
#' can contain arbitrary information, like other data frames, lists, vectors, or
#' models.
#'
#' @param x A data frame containing raw landings data from the Timor operations.
#'
#' @return Landings data in which the information about multiple attachments has
#'   been nested into a single column (`_attachments`) this column contains a
#'   tibble for every row. This, attachment tibble has as many rows as there are
#'   attachments.
#'
#' @export
#' @importFrom rlang .data
#'
#' @examples
#' dummy_landings <- tidyr::tibble(
#'   `_id` = "123",
#'   `_attachments.0.download_url` = "http://url-1.com",
#'   `_attachments.0.id` = "01",
#'   `_attachments.1.download_url` = "http://url-2.com",
#'   `_attachments.1.id` = "02",
#'   `_attachments.2.download_url` = NA,
#'   `_attachments.2.id` = NA
#' )
#' pt_nest_attachments(dummy_landings)
pt_nest_attachments <- function(x) {
  nested_attachments <- x %>%
    # Using the .data pronoun to avoid RMD check notes
    dplyr::select(.data$`_id`, dplyr::starts_with("_attachments")) %>%
    dplyr::mutate_all(as.character) %>%
    # Column names follow the form "_attachments.0.download_large_url"
    tidyr::pivot_longer(
      cols = -.data$`_id`,
      names_to = c("n", "field"),
      names_prefix = "_attachments.",
      names_sep = "\\."
    ) %>%
    # We want one attachment per row and fields as columns
    tidyr::pivot_wider(names_from = "field", values_from = "value") %>%
    # Attachments already have id and this column is superfluous
    dplyr::select(-.data$n) %>%
    dplyr::group_by(.data$`_id`) %>%
    tidyr::nest() %>%
    dplyr::ungroup() %>%
    dplyr::rename("_attachments" = "data") %>%
    # If there are no attachments empty the nested data frames
    dplyr::mutate(`_attachments` = purrr::map(
      .data$`_attachments`,
      ~ dplyr::filter(., !is.na(.data$id))
    ))

  x %>%
    dplyr::select(-dplyr::starts_with("_attachments")) %>%
    dplyr::left_join(nested_attachments, by = "_id")
}
