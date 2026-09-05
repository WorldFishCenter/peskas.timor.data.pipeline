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
  morphometric_tables <- get_morphometric_tables(conf)
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
#' @keywords preprocessing
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
#' @section FAO area filtering:
#' Coefficients are restricted to the FAO major fishing areas in
#' `conf$metadata$fishbase$fao_areas` — **57 and 71** for Timor-Leste, which
#' straddles the Eastern Indian Ocean and the Western Central Pacific. Do not
#' leave that key unset: [coasts::resolve_fao_areas()] falls back to `c(51, 57)`,
#' the *Indian* Ocean pair the WIO pipelines use, which would filter Timor on one
#' wrong area and miss the Pacific one entirely, silently.
#'
#' The filter was off until 2026-09-05, justified as "body form does not stop
#' applying at an area boundary". That answered the wrong question — the filter
#' removes *species* from the pool, it does not invalidate a coefficient. The
#' measured reason to have it on is that the unfiltered pools are dominated by
#' species that do not occur in Timor: `CLP` was priced off 114 records of
#' *Clupea harengus* (Atlantic herring) and 36 of *Sprattus sprattus*.
#'
#' `strip_parentheticals` is on because several FAO names carry a bracketed
#' synonym (`"Haemulidae (=Pomadasyidae)"`) and match nothing as written.
#'
#' @section What is pooled in:
#' Timor's 559 curated rows over 11 mostly-invertebrate codes, from
#' `curated_lw_coeffs()`. They carry no FAO area, so they are bound on **after**
#' the filter and are never area-restricted.
#'
#' @param conf The configuration file.
#'
#' @return A list with `expanded` (the taxon-to-species expansion) and
#'   `length_weight` (`alpha3_code`, `n_studies`, `lw_a`, `lw_b`).
#' @keywords helper
#' @export
get_morphometric_tables <- function(conf) {
  taxa <- get_taxa_list(conf)

  m <- coasts::get_taxa_morphometrics(
    taxa,
    fao_areas = conf$metadata$fishbase$fao_areas,
    filter_by_area = TRUE,
    strip_parentheticals = TRUE
  )

  lw <- summarise_lw_coeffs(
    dplyr::bind_rows(m$length_weight, curated_lw_coeffs())
  )

  assert_taxa_coverage(taxa, lw)

  list(expanded = m$expanded, length_weight = lw)
}

#' Timor's curated length-weight coefficients
#'
#' The 559 hand-curated rows over 11 codes (`COZ CRA CUX FLY GZP IAX MOO OCZ PEZ
#' SFA SLV`, 98 species) that FishBase and SeaLifeBase do not carry — molluscs,
#' crustaceans, echinoderms and seaweed, plus flyingfish. Roughly 4% of national
#' catch weight rests on them.
#'
#' They lived in the Google Sheets `morphometric_table` until 2026-09-05 and are
#' now a package snapshot, refreshed by
#' `data-raw/refresh-morphometric-coefficients.R`. No WIO pipeline has a Sheets
#' metadata source, and this was the weight path's last read of one. Their
#' permanent home is a curated table in `coasts` that
#' [coasts::get_taxa_morphometrics()] pools for every country — COASTS-TODO C23.
#'
#' @return A tibble in [coasts::get_length_weight_coeffs()] shape:
#'   `alpha3_code`, `species_found`, `Type`, `EsQ`, `a`, `b`.
#' @keywords helper
#' @noRd
curated_lw_coeffs <- function() {
  path <- system.file(
    "extdata",
    "morphometric-coefficients.csv",
    package = "peskas.timor.data.pipeline"
  )
  if (!nzchar(path)) {
    stop("inst/extdata/morphometric-coefficients.csv is missing")
  }

  utils::read.csv(path, colClasses = "character") %>%
    dplyr::transmute(
      alpha3_code = .data$alpha3_code,
      species_found = .data$species,
      Type = .data$type,
      EsQ = dplyr::na_if(.data$esq, ""),
      a = as.double(.data$a),
      b = as.double(.data$b)
    ) %>%
    dplyr::as_tibble()
}

#' Fail the run when a taxon loses its coefficients
#'
#' The FishBase fetch is a live network read of a remote parquet dataset with no
#' pinned release, and it has silently returned partial data in production: two
#' consecutive dev runs one day apart differed by 4% in national catch weight,
#' `CJX` and `PWT` dropped out of the coefficient table entirely, and 41 of 51
#' codes moved by up to 54%. Nothing failed, because a taxon with no coefficient
#' pair simply produces `NA` weight, which sums to zero. `CJX` is one of the 13
#' `models.modelled_taxa`.
#'
#' This turns that into a failed job. Two codes are expected to have no
#' coefficients and are exempt: `MZZ` ("Marine fishes nei", the class
#' *Actinopterygii*) and `SWX` ("Seaweeds nei", *Algae*) — neither is a rank the
#' taxonomic backbone carries, and neither has ever had a coefficient here.
#' Mozambique drops `MZZ` from its taxa list outright.
#'
#' @param taxa Output of [get_taxa_list()].
#' @param lw Output of [summarise_lw_coeffs()].
#' @return `lw`, invisibly.
#' @keywords helper
#' @noRd
assert_taxa_coverage <- function(taxa, lw) {
  exempt <- c("MZZ", "SWX")
  missing <- setdiff(setdiff(unique(taxa$alpha3_code), exempt), lw$alpha3_code)

  if (length(missing) > 0) {
    stop(
      "No length-weight coefficients resolved for: ",
      paste(sort(missing), collapse = ", "),
      ". Every catch row of these taxa would weigh NA. This is usually a ",
      "partial FishBase fetch rather than a code change: re-run before ",
      "changing anything."
    )
  }

  logger::log_info(
    "Length-weight coefficients for {nrow(lw)} taxa from ",
    "{sum(lw$n_studies)} published records"
  )
  invisible(lw)
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
      filter_by_area = TRUE,
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
#' [coasts::expand_taxonomic_info()] expects. One row per name to search, so a
#' code may appear more than once — the expansion is an inner join and unions
#' the matches.
#'
#' The **codes** come from the PESKAS | FRAME assets snapshot, which is
#' authoritative for taxa (PLAN §2.5). The **names** come from the FAO ASFIS
#' list, keyed on `Alpha3_Code`, exactly as Mozambique's `process_species_list()`
#' does. The two sources were measured identical on 2026-09-05: 55 of Timor's 56
#' codes carry the same `scientific_name` in both, the exception being `MZZ`
#' (frame `Osteichthyes`, ASFIS `Actinopterygii`), and both expand to the same
#' 50 codes over the same species. ASFIS is used because the WIO pipelines use
#' it, not because it changes an answer.
#'
#' @param conf The configuration file.
#' @return A tibble with `alpha3_code` and `scientific_name`.
#' @keywords helper
#' @noRd
get_taxa_list <- function(conf) {
  codes <- get_assets(conf)$taxa %>%
    timor_assets(conf) %>%
    dplyr::transmute(alpha3_code = as.character(.data$alpha3_code)) %>%
    dplyr::filter(!is.na(.data$alpha3_code)) %>%
    dplyr::distinct()

  asfis <- coasts::download_parquet_from_cloud(
    prefix = conf$metadata$asfis$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country")
  )

  named <- codes %>%
    dplyr::left_join(
      asfis %>%
        dplyr::transmute(
          alpha3_code = as.character(.data$Alpha3_Code),
          # "Pomadasys spp" is a genus written the FAO way; the taxonomic
          # backbone stores the bare genus. Parentheticals are handled by
          # coasts' `strip_parentheticals`.
          scientific_name = sub(
            "\\s+spp\\.?$",
            "",
            as.character(.data$Scientific_Name)
          )
        ),
      by = "alpha3_code"
    )

  unnamed <- named$alpha3_code[is.na(named$scientific_name)]
  if (length(unnamed) > 0) {
    stop(
      "No ASFIS scientific name for: ",
      paste(sort(unnamed), collapse = ", ")
    )
  }

  dplyr::bind_rows(
    named,
    dplyr::filter(taxa_search_aliases(), .data$alpha3_code %in% codes$alpha3_code)
  ) %>%
    dplyr::distinct()
}

#' Extra names to search for codes ASFIS names in a rank FishBase does not carry
#'
#' [coasts::expand_taxonomic_info()] matches against the FishBase / SeaLifeBase
#' backbone at species, genus, family, order and class only. Four of Timor's
#' codes are named at some other rank, or under a name the backbone has since
#' revised, and resolve to nothing on the ASFIS name alone. These rows are added
#' to the search list; they do not replace the ASFIS name, and nothing about the
#' published taxon codes changes.
#'
#' | code | ASFIS name | rank | searched instead |
#' |---|---|---|---|
#' | `TUN` | `Thunnini` | tribe | the tribe's five genera |
#' | `SKH` | `Selachimorpha (Pleurotremata)` | superorder | `Carcharhiniformes` |
#' | `LGE` | `Leiognathus equulus` | superseded binomial | `Leiognathidae` |
#' | `CLP` | `Clupeidae` | family, since split | `Dorosomatidae` too |
#'
#' `TUN` is 56% of landed weight, so its pool is a fishery decision, taken with
#' the user on 2026-09-05. `Thunnini` is the tribe *Allothunnus, Auxis,
#' Euthynnus, Katsuwonus, Thunnus* — which is what Timor's "Tunas nei" lands.
#' Mozambique recodes `TUN` to `TUS` (*Thunnus* alone), which would drop skipjack
#' and frigate tuna; the ASFIS family `Scombridae` would pull in the mackerels
#' Timor already codes separately as `RAX`. Measured at 25 cm: 249 g for the
#' tribe, 249 g for *Thunnus*, 172 g for `Scombridae`.
#'
#' `CLP` is the one that is not a rank problem but a **stale name**. FishBase's
#' 2022 revision moved the tropical sardines — *Sardinella*, *Amblygaster*,
#' *Herklotsichthys*, *Nematalosa*, *Tenualosa* — out of `Clupeidae` into
#' `Dorosomatidae`, while FAO still files them all under `CLUPEIDAE`. What is
#' left in FishBase's `Clupeidae` is 15 mostly temperate species, so Timor's
#' second-largest taxon was being priced off 114 records of *Clupea harengus* and
#' 36 of *Sprattus sprattus*. Searching both families restores 123 area-57/71
#' records over 25 Indo-Pacific species. There is no ASFIS code that says
#' "Indo-Pacific herrings and sardines" *and* resolves in FishBase — `DCX`/`CLU`
#' (`Clupeoidei`) are a suborder and match nothing, `DAG` is the freshwater
#' Lake Tanganyika sardine, and `SIX` (*Sardinella* spp) would rename a published
#' taxon key and drop the herrings the label names. So the fix belongs here and
#' the frame stays as it is.
#'
#' `MZZ` (`Actinopterygii`, a class FishBase files as `Teleostei`) and `SWX`
#' (`Algae`) have no honest substitute and are left unresolved, as they have
#' always been — see [assert_taxa_coverage()]. Mozambique drops `MZZ` outright.
#'
#' @return A tibble with `alpha3_code` and `scientific_name`.
#' @keywords helper
#' @noRd
taxa_search_aliases <- function() {
  dplyr::tribble(
    ~alpha3_code, ~scientific_name,
    "TUN", "Allothunnus",
    "TUN", "Auxis",
    "TUN", "Euthynnus",
    "TUN", "Katsuwonus",
    "TUN", "Thunnus",
    "SKH", "Carcharhiniformes",
    "LGE", "Leiognathidae",
    "CLP", "Dorosomatidae"
  )
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
