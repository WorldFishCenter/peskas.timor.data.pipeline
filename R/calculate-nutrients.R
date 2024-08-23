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
  logger::log_info("Retrieving nutritional values for each taxa group.")
  rfish_tab <- get_rfish_table(pars)
  # get invertebrates nutrients
  fao_groups <- get_fao_composition()

  logger::log_info("Get nutritional fishbase estimates")
  unique_species <- unique(rfish_tab$Species[!is.na(rfish_tab$Species)])

  nutrients_tab <-
    rfishbase::estimate(unique_species) %>% # get updated nutrients values
    dplyr::distinct() %>%
    dplyr::select(
      .data$SpecCode, .data$Calcium, .data$Iron, .data$Omega3,
      .data$Protein, .data$Selenium, .data$VitaminA, .data$Zinc
    ) %>%
    dplyr::right_join(rfish_tab) %>%
    dplyr::select(.data$interagency_code, .data$SpecCode, .data$Calcium:.data$Zinc) %>%
    # na.omit() %>%
    dplyr::group_by(.data$interagency_code, .data$SpecCode) %>%
    dplyr::summarise(dplyr::across(dplyr::everything(), ~ dplyr::first(.x))) %>%
    dplyr::ungroup() %>%
    dplyr::select(
      .data$interagency_code,
      Selenium_mu = .data$Selenium,
      Zinc_mu = .data$Zinc,
      Protein_mu = .data$Protein,
      Omega_3_mu = .data$Omega3,
      Calcium_mu = .data$Calcium,
      Iron_mu = .data$Iron,
      Vitamin_A_mu = .data$VitaminA
    ) %>%
    dplyr::filter(!.data$interagency_code %in% unique(fao_groups$interagency_code)) %>%
    dplyr::bind_rows(fao_groups) %>%
    dplyr::filter(!.data$interagency_code == "FLY")

  # use predicted FLY nutrient values from
  fly_group <-
    dplyr::tibble(
      Selenium_mu = 40.6528,
      Zinc_mu = 1.44887,
      Protein_mu = 17.09566,
      Omega_3_mu = 0.2460711,
      Calcium_mu = 423.1434,
      Iron_mu = 2.109762,
      Vitamin_A_mu = 25.77763,
      interagency_code = "FLY"
    )

  nutrients_tab <-
    nutrients_tab %>%
    dplyr::bind_rows(fly_group)

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

  nutrients_tab %>%
    dplyr::mutate_at(
      dplyr::vars(.data$Selenium_mu:.data$Vitamin_A_mu),
      ~ tidyr::replace_na(., stats::median(., na.rm = TRUE))
    )
  logger::log_info("export nutrients table")
}
#' Get FAO Food Composition Data
#'
#' This function retrieves and processes food composition data from the FAO database.
#' It specifically focuses on marine food items such as octopus, squids, cockles, shrimps,
#' crabs, and lobsters. The data is filtered, categorized for various nutrients
#' including protein, calcium, iron, zinc, selenium, vitamin A, and omega-3 fatty acids.
#'
#' @details
#' The function first reads a CSV file from a given URL using the `read_csv` function from
#' the `readr` package. It then defines specific codes for various marine food groups:
#' octopus, squids, cockles, shrimps, crabs, and lobsters. The FAO composition data is then
#' filtered for these food items in their raw state. A new `interagency_code` is created
#' for categorization purposes. The nutrients considered are protein, calcium,
#' iron, zinc, selenium, vitamin A, and omega-3 fatty acids.
#'
#' @return
#' A tibble with values for various nutrients for each category of marine
#' food items. The columns include `Protein_mu`, `Calcium_mu`, `Iron_mu`, `Zinc_mu`,
#' `Selenium_mu`, `Vitamin_A_mu`, and `Omega_3_mu`.
#'
#' @examples
#' get_fao_composition()
#'
#' @export
get_fao_composition <- function() {
  fao_comp <- readr::read_csv("https://github.com/WorldFishCenter/timor.nutrients/raw/main/inst/fao_food_composition.csv")

  octopus <- c("OCT", "OCT")
  squids <- c("SQZ", "SQR", "OMZ", "CTL", "CTC")
  cockles <- c("CLV", "SVE")
  shrimps <- c("CSH", "PAL", "PAN", "PRA", "PEZ", "ENS", "MPM", "MPN", "PRB", "WKP", "PBA", "GIT", "TIP", "PNV", "SHS")
  crabs <- c("CAD", "DUN", "CRE", "PCR", "SWM", "CRB", "SCD", "MUD")
  lobsters <- c("NEX", "LBA", "LBE", "NEP", "VLO", "LOR")

  fao_comp %>%
    dplyr::rename(interagency_code = .data$integragency_code) %>%
    dplyr::filter(.data$food_state == "r") %>%
    dplyr::filter(.data$interagency_code %in% c(octopus, squids, cockles, shrimps, crabs, lobsters)) %>%
    dplyr::mutate(interagency_code = dplyr::case_when(
      .data$interagency_code %in% octopus ~ "OCZ",
      .data$interagency_code %in% squids ~ "IAX",
      .data$interagency_code %in% cockles ~ "COZ",
      .data$interagency_code %in% shrimps ~ "PEZ",
      .data$interagency_code %in% crabs ~ "CRA",
      .data$interagency_code %in% lobsters ~ "SLV",
      TRUE ~ .data$interagency_code
    )) %>%
    # dplyr::group_by(.data$interagency_code) %>%
    # dplyr::summarise(dplyr::across(.data$`protein(g)`:.data$`omega3(g)`, ~ median(.x, na.rm = TRUE))) %>%
    dplyr::rename(
      Protein_mu = .data$`protein(g)`,
      Calcium_mu = .data$`calcium(mg)`,
      Iron_mu = .data$`iron(mg)`,
      Zinc_mu = .data$`zinc(mg)`,
      Selenium_mu = .data$`selenium(mcg)`,
      Vitamin_A_mu = .data$`vitaminA(mcg)`,
      Omega_3_mu = .data$`omega3(g)`
    ) %>%
    dplyr::select(.data$interagency_code, .data$Protein_mu:.data$Omega_3_mu)
}
