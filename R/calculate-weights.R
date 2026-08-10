#' Add weight of species to merged landings
#'
#' Downloads merged landings and calculates the weight of the catch.
#'
#' The  file is then uploaded to the cloud. The name is the same as for merged
#' landings but with "_weight" at the end. The parameters needed are:
#'
#' ```
#' surveys:
#'   merged_landings:
#'     file_prefix:
#'     version:
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' Progress through the function is tracked using the package *logger*.
#'
#' @param log_threshold The logging threshold level
#' @return No outputs. This function is used for it's side effects.
#' @keywords workflow
#' @export
calculate_weights <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  merged_landings <- get_merged_landings(conf)
  metadata <- get_preprocessed_sheets(conf)
  morphometric_tables <- get_morphometric_tables(
    conf,
    metadata$morphometric_table
  )
  # Reuse the expansion the coefficient fetch already did, rather than paying
  # for a second FishBase round-trip.
  nutrients_table <- get_nutrients_table(
    conf,
    expanded = morphometric_tables$expanded
  ) %>%
    dplyr::rename(species = .data$interagency_code)

  landings_with_weight <- join_weights(
    merged_landings,
    metadata,
    morphometric_tables,
    nutrients_table
  )

  landings_with_weight_filename <- paste(
    conf$surveys$merged_landings$file_prefix,
    "weight",
    sep = "_"
  ) %>%
    add_version(extension = "rds")
  readr::write_rds(
    x = landings_with_weight,
    file = landings_with_weight_filename,
    compress = "gz"
  )

  logger::log_info("Uploading {landings_with_weight_filename} to cloud sorage")
  coasts::upload_cloud_file(
    file = landings_with_weight_filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}






#' Join length-weights and nutritional parameters info to preprocessed landings
#'
#' The function integrate nutritional info from the `get_nutrients_table` function
#' and length-weight info from FIshBase databse to preprocessed surveys data and
#' convert catch  labels according to the FAO nomenclature
#' (\url{http://www.fao.org/fishery/statistics/global-production/3/en}).
#'
#' @section Length types:
#' **Every length reaching this function is a total length.** v2 and v3 record
#' TL; v1 recorded fork length and was converted once, at the freeze
#' (`data-raw/freeze-landings-v1.R`, migration Phase 3).
#'
#' `length_type` is carried through from `catch_types` and is **descriptive
#' only** — it selects nothing and converts nothing. It is non-`NA` for five
#' invertebrate taxa (`SLV` CL, `OCZ` ML, `IAX` ML, `CRA` CW, `COZ` ShL), but
#' enumerators measure those on total length in the field, which is what the
#' `OCZ`/`SLV`/`IAX`/`MOO` overrides below encode. Confirmed 2026-08-10.
#'
#' The prose this replaced claimed the opposite of the code — "total length
#' (TL) in survey version 1 and fork length (FL) in survey version 2" — and an
#' `SRX` → disk-width rule that was never implemented. Both were wrong.
#'
#' @param data The survey landings data frame
#' @param metadata Metadata tables
#' @param rfish_tab Table with length weight parameters
#' @param nutrients_table Table with nutritional parameters
#'
#' @return A new landings data frame including length-weights info
#' @export
#'
join_weights <- function(data, metadata, rfish_tab, nutrients_table) {
  catch_codes <- metadata$catch_types %>%
    dplyr::transmute(
      species = as.character(.data$catch_number),
      catch_taxon = .data$interagency_code,
      length_type = .data$length_type
    ) %>%
    dplyr::mutate(
      catch_taxon = dplyr::if_else(.data$species == "0", "0", .data$catch_taxon)
    )

  data %>%
    dplyr::mutate(
      species_group = purrr::map(
        .x = .data$species_group,
        .f = dplyr::left_join,
        catch_codes,
        by = c("species")
      ),
      species_group = purrr::map(
        .x = .data$species_group,
        .f = dplyr::select,
        -.data$species
      ),
      species_group = purrr::map(
        .x = .data$species_group,
        .f = dplyr::rename,
        species = .data$catch_taxon
      )
    ) %>%
    tidyr::unnest(.data$species_group, keep_empty = TRUE) %>%
    tidyr::unnest(.data$length_individuals, keep_empty = TRUE) %>%
    # fix conditions for "no catch" and "other" labels
    dplyr::mutate(
      species = dplyr::case_when(
        is.na(.data$species) &
          .data$n_individuals > 0 |
          is.na(.data$species) & !.data$total_catch_value == "0" ~ "MZZ",
        is.na(.data$species) &
          is.na(.data$n_individuals) &
          is.na(.data$total_catch_value) |
          is.na(.data$species) &
            is.na(.data$n_individuals) &
            .data$total_catch_value == "0" |
          is.na(.data$species) &
            .data$n_individuals == 0 &
            is.na(.data$total_catch_value) |
          is.na(.data$species) &
            .data$n_individuals == 0 &
            .data$total_catch_value == "0" ~ "0",
        TRUE ~ .data$species
      )
    ) %>%
    # Descriptive only — see the "Length types" section. Every measurement is a
    # total length by the time it gets here, so nothing is converted.
    dplyr::mutate(
      length_type = dplyr::case_when(
        .data$species %in% c("OCZ", "SLV", "IAX", "MOO") ~ "TL",
        !is.na(length_type) ~ length_type,
        TRUE ~ "TL"
      )
    ) %>%
    estimate_weight(rfish_tab$length_weight) %>%
    dplyr::left_join(nutrients_table, by = "species") %>%
    dplyr::mutate(
      weight = abs(.data$weight),
      dplyr::across(
        c(.data$Selenium_mu:.data$Vitamin_A_mu),
        ~ .x * .data$weight
      )
    ) %>%
    tidyr::nest(
      length_individuals = c(
        .data$mean_length,
        .data$n_individuals,
        .data$weight,
        tidyselect::ends_with("_mu")
      )
    ) %>%
    tidyr::nest(
      species_group = c(
        .data$n,
        .data$species,
        .data$food_or_sale,
        .data$other_species_name,
        .data$photo,
        .data$length_individuals,
        .data$length_type
      )
    )
}




#' Build per-taxon length-weight coefficients
#'
#' Fetches coefficients from FishBase and SeaLifeBase via
#' [coasts::get_taxa_morphometrics()] and collapses them to **one coefficient
#' pair per taxon code**, the same aggregation Mozambique uses: geometric mean
#' of `a`, arithmetic mean of `b`.
#'
#' Two flags are non-default and both are required for Timor. `filter_by_area`
#' is off because FAO-area filtering costs ~40% of the species that carry usable
#' coefficients, and body form does not stop applying at an area boundary.
#' `strip_parentheticals` is on because several FAO names carry a bracketed
#' synonym (`"Haemulidae (=Pomadasyidae)"`) and match nothing as written.
#'
#' @param conf The configuration file.
#' @param manual_table Timor's curated morphometric rows
#'   (`metadata$morphometric_table`), pooled with the fetched coefficients
#'   before aggregation.
#'
#' @return A list with `expanded` (the taxon-to-species expansion) and
#'   `length_weight` (`alpha3_code`, `n_studies`, `lw_a`, `lw_b`).
#' @keywords helper
#' @export
get_morphometric_tables <- function(conf, manual_table = NULL) {
  taxa <- get_taxa_list(conf)

  m <- coasts::get_taxa_morphometrics(
    taxa,
    fao_areas = conf$metadata$fishbase$fao_areas,
    filter_by_area = FALSE,
    strip_parentheticals = TRUE
  )

  lw <- m$length_weight

  # expand_taxonomic_info() matches the FishBase/SeaLifeBase backbone, so it
  # resolves nothing for tribes or informal groupings. These three are reachable
  # by common name only.
  lw <- dplyr::bind_rows(lw, rescue_by_common_name(taxa))

  if (!is.null(manual_table)) {
    lw <- dplyr::bind_rows(
      lw,
      dplyr::transmute(
        manual_table,
        alpha3_code = as.character(.data$interagency_code),
        Type = as.character(.data$Type),
        EsQ = as.character(.data$EsQ),
        a = as.double(.data$a),
        b = as.double(.data$b)
      )
    )
  }

  list(
    expanded = m$expanded,
    length_weight = summarise_lw_coeffs(lw)
  )
}

#' Taxon-to-species expansion, in the column names the nutrients code expects
#'
#' @param conf The configuration file.
#' @param expanded Optional pre-computed `expanded` table from
#'   [get_morphometric_tables()], to avoid a second FishBase round-trip.
#' @return A tibble: `interagency_code`, `Species`, `SpecCode`.
#' @keywords helper
#' @noRd
get_taxa_expansion <- function(conf, expanded = NULL) {
  if (is.null(expanded)) {
    expanded <- coasts::get_taxa_morphometrics(
      get_taxa_list(conf),
      fao_areas = conf$metadata$fishbase$fao_areas,
      filter_by_area = FALSE,
      strip_parentheticals = TRUE
    )$expanded
  }

  expanded %>%
    dplyr::transmute(
      interagency_code = .data$alpha3_code,
      Species = .data$species_found,
      SpecCode = .data$SpecCode
    ) %>%
    dplyr::distinct()
}

#' Taxon list driving the coefficient fetch
#'
#' `alpha3_code` + `scientific_name`, the pair
#' [coasts::expand_taxonomic_info()] expects.
#'
#' Sourced from the PESKAS | FRAME assets snapshot, which is authoritative for
#' taxa. It replaced the Google Sheets `catch_types` + `fao_catch` join in
#' migration Phase 3; the two were measured identical — 56 codes each, zero
#' differing `scientific_name` values.
#'
#' @param conf The configuration file.
#' @return A tibble with `alpha3_code` and `scientific_name`.
#' @keywords helper
#' @noRd
get_taxa_list <- function(conf) {
  get_assets(conf)$taxa %>%
    timor_assets(conf) %>%
    dplyr::transmute(
      alpha3_code = as.character(.data$alpha3_code),
      # "Pomadasys spp" is a genus written the FAO way; the taxonomic backbone
      # stores the bare genus. Parentheticals are handled by coasts'
      # `strip_parentheticals`.
      scientific_name = sub(
        "\\s+spp\\.?$",
        "",
        as.character(.data$scientific_name)
      )
    ) %>%
    dplyr::filter(!is.na(.data$alpha3_code), !is.na(.data$scientific_name)) %>%
    dplyr::distinct()
}

#' Recover taxa the taxonomic backbone cannot resolve
#'
#' `Thunnini` (tribe), `Selachimorpha` (informal) and `Hyporhamphus quoyi`
#' (superseded binomial) return nothing from the FishBase/SeaLifeBase backbone.
#' [rfishbase::common_to_sci()] reaches them.
#'
#' @param taxa Output of [get_taxa_list()].
#' @return A tibble in `coasts::get_length_weight_coeffs()` shape.
#' @keywords helper
#' @noRd
rescue_by_common_name <- function(taxa) {
  overrides <- c(TUN = "Tuna", SKH = "Shark", GZP = "Garfish")
  overrides <- overrides[names(overrides) %in% taxa$alpha3_code]
  if (length(overrides) == 0) {
    return(NULL)
  }

  purrr::imap_dfr(overrides, function(common, code) {
    species <- tryCatch(
      unique(rfishbase::common_to_sci(common)$Species),
      error = function(e) character(0)
    )
    if (length(species) == 0) {
      logger::log_warn("No species found for {code} via common name {common}")
      return(NULL)
    }
    rfishbase::length_weight(species) %>%
      dplyr::transmute(
        alpha3_code = code,
        Type = .data$Type,
        EsQ = .data$EsQ,
        a = .data$a,
        b = .data$b
      )
  })
}

#' Collapse length-weight coefficients to one pair per taxon
#'
#' Geometric mean of `a`, arithmetic mean of `b` — `a` is a scale factor whose
#' published values span orders of magnitude, so it is averaged in log space,
#' while `b` is an exponent clustered near 3.
#'
#' No filtering on `Type` is applied. Restricting to `Type == "TL"` discards
#' more than half the matched species for `CJX`, `EMP`, `MOB` and `YDX`.
#' Low-quality studies (`EsQ == "yes"`) are dropped, as they were before.
#'
#' @param lw Length-weight rows from [coasts::get_length_weight_coeffs()].
#' @return A tibble: `alpha3_code`, `n_studies`, `lw_a`, `lw_b`.
#' @keywords helper
#' @noRd
summarise_lw_coeffs <- function(lw) {
  lw %>%
    dplyr::filter(
      !is.na(.data$a),
      !is.na(.data$b),
      .data$a > 0,
      is.na(.data$EsQ) | tolower(.data$EsQ) != "yes"
    ) %>%
    dplyr::group_by(.data$alpha3_code) %>%
    dplyr::summarise(
      n_studies = dplyr::n(),
      lw_a = exp(mean(log(.data$a))),
      lw_b = mean(.data$b),
      .groups = "drop"
    )
}

#' Estimate catch weight from length
#'
#' `W = a * L^b * N`, the relationship Mozambique's `calculate_catch_adnap()`
#' applies.
#'
#' **Units are grams.** FishBase publishes `a` for a result in grams, and the
#' portal export divides by 1000 downstream. Do not convert here.
#'
#' @param data Catch rows carrying `species`, `mean_length` and `n_individuals`.
#' @param lw Output of [summarise_lw_coeffs()].
#' @return `data` with `weight` added, in grams.
#' @keywords helper
#' @noRd
estimate_weight <- function(data, lw) {
  data %>%
    dplyr::left_join(
      dplyr::select(lw, species = "alpha3_code", "lw_a", "lw_b"),
      by = "species"
    ) %>%
    dplyr::mutate(
      weight = dplyr::if_else(
        !is.na(.data$mean_length) &
          !is.na(.data$lw_a) &
          !is.na(.data$lw_b) &
          !is.na(.data$n_individuals),
        .data$lw_a * .data$mean_length^.data$lw_b * .data$n_individuals,
        NA_real_
      ),
      weight = dplyr::if_else(.data$n_individuals == 0, 0, .data$weight)
    ) %>%
    dplyr::select(-"lw_a", -"lw_b")
}
