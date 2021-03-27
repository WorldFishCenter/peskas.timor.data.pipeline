#' Nest survey columns in landings data
#'
#' `pt_nest_attachments()` nests attachment data in a kobo survey.
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
#' @author Fernando Cagua
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
#'   `_attachments.2.id` = NA)
#' pt_nest_attachments(dummy_landings)
#'
pt_nest_attachments <- function(x){

  nested_attachments <- x %>%
    # Using the .data pronoun to avoid RMD check notes
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
    dplyr::left_join(nested_attachments, by = "_id")
}


#' Nest species columns
#'
#' Nests species columns obtained when reading structured data from the kobo
#' landings survey
#'
#' @param x
#' @inheritParams pt_nest_attachments
#' @inherit pt_nest_attachments details
#'
#' @return Landings data in which the information about multiple species has
#'   been nested into a single column (`species_group`) this column contains a
#'   tibble for every row. This, species tibble has as many rows as there are
#'   species. In turn, the data frames contained in `species_group` contain a
#'   column called `length_individuals` where a data frame with the number of
#'   individuals per length is stored
#'
#' @export
#' @author Fernando Cagua
#' @importFrom rlang .data
#'
#' @examples
#' dummy_landings <- tidyr::tibble(
#'     `_id` = "123",
#'     `species_group.0.species_group/species` = "sp01",
#'     `species_group.0.species_group/food_or_sale` = "food",
#'     `species_group.0.species_group/no_fish_by_length_group/no_individuals_10_15` = 10,
#'     `species_group.0.species_group/no_fish_by_length_group/no_individuals_5_10` = 10,
#'     `species_group.0.species_group/no_fish_by_length_group/fish_length_over60` = 80,
#'     `species_group.0.species_group/no_fish_by_length_group/no_individuals_over60` = 1,
#'     `species_group.1.species_group/species` = "sp02",
#'     `species_group.1.species_group/food_or_sale` = "sale",
#'     `species_group.1.species_group/no_fish_by_length_group/no_individuals_20_25` = 5,
#'     `species_group.1.species_group/no_fish_by_length_group/no_individuals_5_10` = 20,
#'     `species_group.1.species_group/no_fish_by_length_group/fish_length_over60` = 70,
#'     `species_group.1.species_group/no_fish_by_length_group/no_individuals_over60` = 2)
#' pt_nest_species(dummy_landings)
pt_nest_species <- function(x){

  # Nest species
  nested_species <- x %>%
    # Using the .data pronoun to avoid RMD check notes
    dplyr::select(.data$`_id`, dplyr::starts_with("species_group")) %>%
    dplyr::mutate_all(as.character) %>%
    # Column names follow the form "species_group.3.species_group/food_or_sale"
    tidyr::pivot_longer(cols = -.data$`_id`,
                        names_to = c("dummy", "n", "var"),
                        names_sep = "\\.") %>%
    # We want one specis per row
    tidyr::pivot_wider(names_from = "var", values_from = "value") %>%
    dplyr::select(-.data$dummy) %>%
    # Column names for the number of individuals per length class follow
    # "species_group.3.species_group/no_fish_by_length_group/no_individuals_10_15"
    dplyr::rename_all(~stringr::str_remove(., "species_group/")) %>%
    # We want one row per length class
    tidyr::pivot_longer(cols = dplyr::starts_with("no_fish_by_length_group")) %>%
    dplyr::group_by(.data$`_id`, .data$n) %>%
    # If individuals were larger than 60cm the length is recorded in the
    # "no_fish_by_length_group/fish_length_over60" column. Then instead of
    # length categories we estimate the mean point for each category which allow
    # us to include the number of individuals over 60 as just one more row
    dplyr::mutate(
      length_over_60 =
        .data$value[.data$name == "no_fish_by_length_group/fish_length_over60"],
      mean_length = stringr::str_extract_all(.data$name, "((?<=_|r)[0-9]+)"),
      mean_length = purrr::map(.data$mean_length, as.numeric),
      mean_length = purrr::map_dbl(.data$mean_length, mean),
      mean_length = dplyr::if_else(.data$mean_length == 60 &
                                     !is.na(.data$length_over_60),
                                   as.numeric(.data$length_over_60),
                                   .data$mean_length),
      n_individuals = as.numeric(.data$value)) %>%
    # Once the length of fish over 60 has been incorporated on its own row as
    # the others we don't need that col anymore
    dplyr::filter(.data$name != "no_fish_by_length_group/fish_length_over60") %>%
    dplyr::select(-.data$name, -.data$value, -.data$length_over_60) %>%
    dplyr::group_by(.data$`_id`, .data$n) %>%
    # Back to one row per species
    tidyr::nest(length_individuals = c(.data$mean_length, .data$n_individuals)) %>%
    dplyr::group_by(.data$`_id`) %>%
    # Back to one row per trip
    tidyr::nest() %>%
    dplyr::rename("species_group" = "data") %>%
    # Remove rows in the nested column that are empty (no species recorded)
    dplyr::mutate(species_group = purrr::map(
      .data$species_group,
      ~ dplyr::filter(., !is.na(.data$species))))

  x %>%
    dplyr::select(-dplyr::starts_with("species_group")) %>%
    dplyr::left_join(nested_species, by = "_id")
}
