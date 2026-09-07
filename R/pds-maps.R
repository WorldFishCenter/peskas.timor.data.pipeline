#' PDS-derived map products
#'
#' Everything in this file is Timor-only and downstream of PDS, not PDS
#' ingestion: migration Phase 7 deleted `ingest_pds_trips()`,
#' `ingest_pds_tracks()` and `preprocess_pds_trips()` in favour of
#' `coasts::ingest_pds_trips()`, `coasts::ingest_pds_tracks()` and
#' `coasts::preprocess_pds_tracks()`, which is how Mozambique, Kenya and
#' Zanzibar have always done it — none of them carries any PDS code.
#'
#' What is left are the boundary and taxa-name lookups the map products used to
#' share with `format_public_data()`.
#'
#' Phase 8 resolved the dangling dependency the Phase 7 handover described. Of
#' the two map products, `tracks-map.png` (last written 2021-12-11) turned out
#' to have no reader at all, and `indicators_gridded.rds` (last written
#' 2024-07-27 in production) had exactly one: `export_files()`, which rebuilt
#' `portal-indicators_grid.json` and `portal-label_groups_list.json` from it on
#' every run. Those are the two objects `peskas.timor.portal.v2` explicitly
#' excludes, so Phase 8 dropped them — which left `ingest_pds_map()`,
#' `ingest_kepler_tracks()`, `kepler_mapper()` and `ingest_complete_tracks()`
#' unreferenced. **Phase 11 deleted all four**, with `inst/kepler_mapper.py`
#' and the accessors that fed them. What is left is
#' `get_timor_boundaries()` and `convert_taxa_names()`, both of which
#' `format_public_data()` calls.
#'
#' @keywords internal
#' @name pds-maps
NULL

# NOTE: `ingest_complete_tracks()`, `ingest_pds_map()`, `ingest_kepler_tracks()`
# and `kepler_mapper()` lived here until migration Phase 11, together with
# `inst/kepler_mapper.py`. All four were unreferenced once Phase 8 stopped
# `export_files()` reading `indicators_gridded`; restore from git history if a
# map product is ever wanted again.

#' Convert taxa codes to common names
#'
#' @param data A dataframe with taxa codes under a column named "catch_taxon"
#' @param conf The config file
#'
#' @return A dataframe with taxa common names
#' @keywords helper
#' @export
#'
convert_taxa_names <- function(data, conf) {
  catch_types <-
    peskas.timor.data.pipeline::get_preprocessed_sheets(conf)$catch_types %>%
    dplyr::filter(
      !.data$catch_name_en %in%
        c("Herring", "Unknown", "Surgeonfish", "Bannerfish", "No catch")
    ) %>%
    dplyr::select(
      catch_taxon = .data$interagency_code,
      "Common name" = .data$catch_name_en
    ) %>%
    dplyr::mutate(
      "Common name" = dplyr::case_when(
        catch_taxon == "RAX" ~ "Short mackerel",
        catch_taxon == "CGX" ~ "Jacks/Trevally",
        catch_taxon == "CLP" ~ "Sardines",
        catch_taxon == "TUN" ~ "Tuna/Bonito",
        catch_taxon == "SNA" ~ "Snapper",
        TRUE ~ .data$`Common name`
      )
    )
  data %>%
    dplyr::left_join(catch_types, by = "catch_taxon") %>%
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
    ) %>%
    dplyr::select(-.data$catch_taxon) %>%
    dplyr::rename(catch_taxon = .data$`Common name`)
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
