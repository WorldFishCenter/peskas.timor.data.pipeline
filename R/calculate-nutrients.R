#' Get taxa nutritional values
#'
#' Download the modelled nutrional properties of each taxa according to
#' Hicks et al., 2019 (\url{https://www.nature.com/articles/s41586-019-1592-6}).
#' The table of nutrients for each taxon is directly called from the paper
#' repository (\url{https://github.com/mamacneil/NutrientFishbase}).
#'
#' Starting units for each nutrient:
#'  - mg/100g: Calcium, Iron and Zinc.
#'  - μg/100g: Selenium and Vitamin A.
#'  - g/100g: Omega 3 and Protein.
#'
#'
#' @param pars The configuration file.
#' @param summarise Whether you want to summarise the nutritional values for each
#' group. The function use the median by default.
#' @param convert Whether you want to convert the nutritional values units to
#' grams. See details for the default starting units.
#'
#'
#' @return A data frame with nutritional values for each taxa group.
#' @export
#'
get_nutrients_table <- function(pars, summarise = TRUE, convert = TRUE) {
  rfish_tab <- get_rfish_table(pars)

  nutrients_tab <-
    readr::read_csv(pars$metadata$nutrients$resource,
      show_col_types = FALSE
    ) %>%
    dplyr::rename(SpecCode = .data$spec_code) %>%
    dplyr::mutate(SpecCode = as.integer(.data$SpecCode)) %>%
    dplyr::select(.data$species, .data$SpecCode, tidyselect::contains("_mu")) %>%
    dplyr::right_join(rfish_tab, by = "SpecCode") %>%
    dplyr::select(.data$interagency_code, tidyselect::contains("_mu"))

  if (isTRUE(convert)) {
    nutrients_tab <-
      nutrients_tab %>%
      dplyr::mutate(dplyr::across(
        c(.data$Zinc_mu, .data$Calcium_mu, .data$Iron_mu),
        ~ (.x / 1000) / 100
      )) %>%
      dplyr::mutate(dplyr::across(
        c(.data$Selenium_mu, .data$Vitamin_A_mu),
        ~ (.x / 1000000) / 100
      )) %>%
      dplyr::mutate(dplyr::across(
        c(.data$Omega_3_mu, .data$Protein_mu),
        ~ (.x / 1) / 100
      ))
  }
  if (isTRUE(summarise)) {
    nutrients_tab <-
      nutrients_tab %>%
      dplyr::group_by(.data$interagency_code) %>%
      dplyr::summarise_all(stats::median, na.rm = TRUE)
  }

  nutrients_tab
}
