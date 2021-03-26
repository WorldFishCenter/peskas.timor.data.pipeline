#' Nest survey attachment columns in landings data
#'
#' @param x A data frame containing landings data from the Timor operations
#'   in which the attachments information is spread across multiple columns
#'
#' @return Landings data in which the information about multiple attachments has
#'   been nested into a single column
#' @author Fernando Cagua
#' @export
#'
#' @importFrom rlang .data
#'
#' @example
#' dummy_landings <- tidyr::tibble(
#'   `_id` = "123",
#'   `_attachments.0.download_url` = "http://url-1.com",
#'   `_attachments.0.id` = "01",
#'   `_attachments.1.download_url` = "http://url-2.com",
#'   `_attachments.1.id` = "02",
#'   `_attachments.2.download_url` = NA,
#'   `_attachments.2.id` = NA)
#' pt_nest_attachments(dummy_landings)
#'
pt_nest_attachments <- function(x){

  nested_attachments <- x %>%
    dplyr::select(.data$`_id`, dplyr::starts_with("_attachments")) %>%
    # Column names follow the form "_attachments.0.download_large_url"
    tidyr::pivot_longer(cols = -.data$`_id`,
                        names_to = c("n", "field"),
                        names_prefix = "_attachments.",
                        names_sep = "\\.") %>%
    # We want one attachment per row and fields as columns
    tidyr::pivot_wider(names_from = "field", values_from = "value") %>%
    # Attachments already have id and this column is superfluous
    dplyr::select(-.data$n) %>%
    dplyr::group_by(.data$`_id`) %>%
    tidyr::nest() %>%
    dplyr::ungroup() %>%
    dplyr::rename("_attachments" = "data") %>%
    # If there are no attachments empty the nested data frames
    dplyr::mutate(`_attachments` = purrr::map(.data$`_attachments`,~ dplyr::filter(., !is.na(.data$id))))

  x %>%
    dplyr::select(-dplyr::starts_with("_attachments")) %>%
    dplyr::left_join(nested_attachments)
}

