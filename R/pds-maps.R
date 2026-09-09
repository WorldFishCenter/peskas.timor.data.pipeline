#' Boundary and taxa lookups for the public data export
#'
#' The two lookups [get_timor_boundaries()] and [convert_taxa_names()], both
#' called by [format_public_data()].
#'
#' @keywords internal
#' @name pds-maps
NULL

#' Assign each taxon code to a broad species group
#'
#' @param data A dataframe with taxa codes under a column named "catch_taxon"
#'
#' @return `data` with a `fish_group` column
#' @keywords helper
#' @export
#'
convert_taxa_names <- function(data) {
  data %>%
    dplyr::mutate(
      fish_group = dplyr::case_when(
        catch_taxon %in% c("COZ") ~ "Molluscs",
        catch_taxon %in% c("PEZ") ~ "Shrimps",
        catch_taxon %in% c("MZZ") ~ "Unknown",
        catch_taxon %in% c("SLV", "CRA") ~ "Crustaceans",
        catch_taxon %in% c("OCZ", "IAX") ~ "Cephalopods",
        catch_taxon %in% c("SKH", "SRX") ~ "Sharks and rays",
        catch_taxon %in%
          c(
            "SNA",
            "GPX",
            "PWT",
            "SUR",
            "GRX",
            "MUI",
            "BGX"
          ) ~ "Large demersals",
        catch_taxon %in%
          c(
            "CGX",
            "TUN",
            "BEN",
            "LWX",
            "BAR",
            "SFA",
            "CBA",
            "DOX",
            "ECN",
            "DOS"
          ) ~ "Large pelagics",
        catch_taxon %in%
          c(
            "YDX",
            "SPI",
            "EMP",
            "SUR",
            "TRI",
            "MOJ",
            "WRA",
            "MOO",
            "BWH",
            "LGE",
            "MOB",
            "MHL",
            "GOX",
            "THO",
            "IHX",
            "APO",
            "IHX",
            "PUX",
            "DRZ"
          ) ~ "Small demersals",
        catch_taxon %in%
          c(
            "RAX",
            "SDX",
            "CJX",
            "CLP",
            "GZP",
            "FLY",
            "KYX",
            "CLP",
            "MUL",
            "DSF",
            "MIL",
            "THF"
          ) ~ "Small pelagics",
        TRUE ~ NA_character_
      )
    )
}

#' Extract Timor Geographical Boundaries
#'
#' This function extracts the geographical boundaries of Timor, including national boundaries,
#' administrative regions, and special cases for Atauro and Dili. It uses shapefiles stored within
#' the 'peskas.timor.data.pipeline' package to perform this extraction. Logging is incorporated to
#' inform the user of the process status and any potential threshold-based logging details.
#'
#' @param log_threshold The logging threshold level as defined by the `logger` package. This parameter
#'        allows the user to set the verbosity of logging output. The default level is `logger::DEBUG`.
#'
#' @return An `sf` object containing the geographical boundaries of Timor's regions, including adjustments
#'         for specific areas like Atauro and Dili. The returned object includes region names and their
#'         corresponding geometries.
#'
#' @keywords helper
#' @export
#'
#' @examples
#' # Assuming 'peskas.timor.data.pipeline' package is installed and loaded
#' timor_boundaries <- get_timor_boundaries()
#' plot(timor_boundaries$geometry)
#'
get_timor_boundaries <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  logger::log_info("Extracting Timor shape boundaries...")

  timor_nation <- sf::read_sf(system.file(
    "report/timor_shapefiles/tls_admbnda_adm0_who_ocha_20200911.shp",
    package = "peskas.timor.data.pipeline"
  ))
  timor_atauro <-
    sf::read_sf(system.file(
      "report/timor_shapefiles/tls_admbnda_adm2_who_ocha_20200911.shp",
      package = "peskas.timor.data.pipeline"
    )) %>%
    dplyr::filter(.data$ADM2_EN == "Atauro") %>%
    dplyr::rename(region = .data$ADM2_EN) %>%
    dplyr::select(-c("ADM0_EN", "ADM0_PCODE"))

  timor_dili <-
    sf::read_sf(system.file(
      "report/timor_shapefiles/tls_admbnda_adm2_who_ocha_20200911.shp",
      package = "peskas.timor.data.pipeline"
    )) %>%
    dplyr::rename(region = .data$ADM1_EN) %>%
    dplyr::filter(.data$region == "Dili" & !.data$ADM2_EN == "Atauro") %>%
    dplyr::summarise(region = "Dili")

  timor_region <-
    sf::read_sf(system.file(
      "report/timor_shapefiles/tls_admbnda_adm1_who_ocha_20200911.shp",
      package = "peskas.timor.data.pipeline"
    )) %>%
    dplyr::rename(region = .data$ADM1_EN) %>%
    dplyr::mutate(
      region = dplyr::case_when(
        .data$region == "Laut\u00E9m" ~ "Lautem",
        .data$region == "Liqui\u00E7\u00E1" ~ "Liquica",
        .data$region == "Oecussi" ~ "Oecusse",
        TRUE ~ .data$region
      )
    ) %>%
    dplyr::filter(!.data$region %in% c("Ermera", "Aileu", "Atauro", "Dili")) %>%
    dplyr::bind_rows(timor_atauro) %>%
    dplyr::bind_rows(timor_dili) %>%
    dplyr::select(.data$region, .data$geometry)
  timor_region
}
