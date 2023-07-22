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
#' @importFrom rlang .data
#'
#' @examples
#' dummy_landings <- tidyr::tibble(
#'   `_id` = "123",
#'   `species_group.0.species_group/species` = "sp01",
#'   `species_group.0.species_group/food_or_sale` = "food",
#'   `species_group.0.species_group/no_fish_by_length_group/no_individuals_10_15` = 10,
#'   `species_group.0.species_group/no_fish_by_length_group/no_individuals_5_10` = 10,
#'   `species_group.0.species_group/no_fish_by_length_group/fish_length_over60` = 80,
#'   `species_group.0.species_group/no_fish_by_length_group/no_individuals_over60` = 1,
#'   `species_group.1.species_group/species` = "sp02",
#'   `species_group.1.species_group/food_or_sale` = "sale",
#'   `species_group.1.species_group/no_fish_by_length_group/no_individuals_20_25` = 5,
#'   `species_group.1.species_group/no_fish_by_length_group/no_individuals_5_10` = 20,
#'   `species_group.1.species_group/no_fish_by_length_group/fish_length_over60` = 70,
#'   `species_group.1.species_group/no_fish_by_length_group/no_individuals_over60` = 2
#' )
#' pt_nest_species(dummy_landings)
pt_nest_species <- function(x) {
  # Nest species
  nested_species <- x %>%
    # Using the .data pronoun to avoid RMD check notes
    dplyr::select(.data$`_id`, dplyr::starts_with("species_group")) %>%
    dplyr::mutate_all(as.character) %>%
    # Column names follow the form "species_group.3.species_group/food_or_sale"
    tidyr::pivot_longer(
      cols = -.data$`_id`,
      names_to = c("dummy", "n", "var"),
      names_sep = "\\."
    ) %>%
    # We want one specis per row
    tidyr::pivot_wider(names_from = "var", values_from = "value") %>%
    dplyr::select(-.data$dummy) %>%
    # Column names for the number of individuals per length class follow
    # "species_group.3.species_group/no_fish_by_length_group/no_individuals_10_15"
    dplyr::rename_all(~ stringr::str_remove(., "species_group/")) %>%
    # We want one row per length class
    tidyr::pivot_longer(cols = dplyr::starts_with("no_fish_by_length_group"))

  if (any(grepl("fish_length_over60", names(nested_species)))) {
    # If individuals were larger than 60cm the length is recorded in the
    # "no_fish_by_length_group/fish_length_over60" column. Then instead of
    # length categories we estimate the mean point for each category which allow
    # us to include the number of individuals over 60 as just one more row
    nested_species <-
      nested_species %>%
      dplyr::group_by(.data$`_id`, .data$n) %>%
      dplyr::mutate(
        length_over_60 =
          .data$value[.data$name == "no_fish_by_length_group/fish_length_over60"],
        mean_length = stringr::str_extract_all(.data$name, "((?<=_|r)[0-9]+)"),
        mean_length = purrr::map(.data$mean_length, as.numeric),
        mean_length = purrr::map_dbl(.data$mean_length, mean),
        mean_length = dplyr::if_else(.data$mean_length == 60 &
          !is.na(.data$length_over_60),
        as.numeric(.data$length_over_60),
        .data$mean_length
        ),
        n_individuals = as.numeric(.data$value)
      ) %>%
      # Once the length of fish over 60 has been incorporated on its own row as
      # the others we don't need that col anymore
      dplyr::filter(.data$name != "no_fish_by_length_group/fish_length_over60") %>%
      dplyr::select(-.data$name, -.data$value, -.data$length_over_60) %>%
      tidyr::nest(length_individuals = c(.data$mean_length, .data$n_individuals)) %>%
      dplyr::group_by(.data$`_id`) %>%
      # Back to one row per trip
      tidyr::nest() %>%
      dplyr::rename("species_group" = "data") %>%
      # Remove rows in the nested column that are empty (no species recorded)
      dplyr::mutate(species_group = purrr::map(
        .data$species_group,
        ~ dplyr::filter(., !is.na(.data$species))
      ))
  } else {
    nested_species <-
      nested_species %>%
      dplyr::group_by(.data$`_id`, .data$n) %>%
      dplyr::mutate(
        mean_length = stringr::str_extract_all(.data$name, "((?<=_|r)[0-9]+)"),
        mean_length = purrr::map(.data$mean_length, as.numeric),
        mean_length = purrr::map_dbl(.data$mean_length, mean),
        n_individuals = as.numeric(.data$value)
      ) %>%
      # Back to one row per species
      dplyr::select(-.data$name, -.data$value) %>%
      tidyr::nest(length_individuals = c(.data$mean_length, .data$n_individuals)) %>%
      dplyr::group_by(.data$`_id`) %>%
      # Back to one row per trip
      tidyr::nest() %>%
      dplyr::rename("species_group" = "data") %>%
      # Remove rows in the nested column that are empty (no species recorded)
      dplyr::mutate(species_group = purrr::map(
        .data$species_group,
        ~ dplyr::filter(., !is.na(.data$species))
      ))
  }

  x %>%
    dplyr::select(-dplyr::starts_with("species_group")) %>%
    dplyr::left_join(nested_species, by = "_id")
}
