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
    dplyr::rename(catch_taxon = .data$interagency_code)

  landings_with_weight <- join_weights(
    merged_landings,
    morphometric_tables,
    nutrients_table
  )

  logger::log_info(
    "Uploading {nrow(landings_with_weight)} weighted catch rows to cloud storage"
  )
  coasts::upload_parquet_to_cloud(
    data = landings_with_weight,
    prefix = conf$surveys$landings$weight$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country")
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
#' @section Input and output shape:
#' Both are the **flat long** table — one row per (submission, catch, length
#' bin) — with the taxon already resolved from the assets snapshot by
#' [preprocess_landings()]. The output adds `weight` (grams) and the seven
#' per-catch nutrient columns and changes nothing else.
#'
#' Until migration Phase 5 this function also re-nested the catch columns into
#' `species_group` / `length_individuals` and dropped the standard submission
#' columns, purely so the validators could keep reading raw KoBo names off a
#' legacy-shaped artefact. [validate_landings()] reads the long table now, so
#' the bridge is gone and the artefact is parquet like every stage before it.
#'
#' @param data The merged long landings table
#' @param rfish_tab Table with length weight parameters
#' @param nutrients_table Table with nutritional parameters
#'
#' @return The long landings table with `weight` and nutrients added
#' @export
#'
join_weights <- function(data, rfish_tab, nutrients_table) {
  data %>%
    estimate_weight(rfish_tab$length_weight) %>%
    dplyr::left_join(nutrients_table, by = "catch_taxon") %>%
    dplyr::mutate(
      weight = abs(.data$weight),
      dplyr::across(tidyselect::ends_with("_mu"), ~ .x * .data$weight)
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
#' @param data Catch rows carrying `catch_taxon`, `length` and `n_individuals`.
#' @param lw Output of [summarise_lw_coeffs()].
#' @return `data` with `weight` added, in grams.
#' @keywords helper
#' @noRd
estimate_weight <- function(data, lw) {
  data %>%
    dplyr::left_join(
      dplyr::select(lw, catch_taxon = "alpha3_code", "lw_a", "lw_b"),
      by = "catch_taxon"
    ) %>%
    dplyr::mutate(
      weight = dplyr::if_else(
        !is.na(.data$length) &
          !is.na(.data$lw_a) &
          !is.na(.data$lw_b) &
          !is.na(.data$n_individuals),
        .data$lw_a * .data$length^.data$lw_b * .data$n_individuals,
        NA_real_
      ),
      weight = dplyr::if_else(.data$n_individuals == 0, 0, .data$weight)
    ) %>%
    dplyr::select(-"lw_a", -"lw_b")
}
