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
#' (done once in the freeze).
#'
#' @section Input and output shape:
#' Both are the **flat long** table — one row per (submission, catch, length
#' bin) — with the taxon already resolved from the assets snapshot by
#' [preprocess_landings()]. The output adds `weight` (grams) and the seven
#' per-catch nutrient columns and changes nothing else.

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
#' @section Length types:
#' A published pair `W = a * L^b` is fitted on whichever axis the study used —
#' `Type` is `TL`, `FL`, `SL`, `CL`, … — but every length reaching
#' [join_weights()] is a **total length**. Pooling the pairs as published
#' therefore averaged fork-length and standard-length fits into one number and
#' applied it to TL, which overestimates weight, because FL and SL are shorter
#' than TL (medians here: FL 0.958 TL, SL 0.827 TL).
#'
#' [coasts::convert_lw_to_tl()] restates every convertible pair on a TL basis,
#' from the `length_length` table fetched by the same call. **`b` is unchanged
#' and only `a` is rescaled**, by `ratio^b`. The POPLL fit direction, the 1 cm
#' intercept cut-off and the median-ratio-per-species-and-type rule all live
#' there, asserted by its tests rather than restated here.
#'
#' `length_types = NULL` is passed for this reason. The coasts default keeps
#' only `TL`/`FL` pairs, which would leave the 460 `SL` rows unconvertible —
#' and `SL` is where the error is largest.
#'
#' Measured on Timor's taxa: of 1,648 non-TL pairs, 1,363 convert and 285 do
#' not. **The 285 are kept as published rather than dropped.** Dropping them
#' takes `MOO` down 92% and `SFA` 78% — those taxa's fetched pairs carry them,
#' and what is left is a curated supplement fitted on a different axis again.
#' Zanzibar 4.9.0 converts only for taxa that would otherwise have nothing;
#' Timor converts everything it can, which is why no `Type` filter is needed
#' here (see `summarise_lw_coeffs()`).
#'
#' @section What is pooled in:
#' Timor's 559 curated rows over 11 mostly-invertebrate codes, from
#' `curated_lw_coeffs()`. They carry no FAO area, so they are bound on **after**
#' the filter and are never area-restricted — and, being bound after the
#' conversion above, they are never restated either. That is deliberate: their
#' `Type` values (`CW`, `ShL`, `ML`, `CL`) are invertebrate axes FishBase
#' carries no conversion for, and field practice measures those taxa on total
#' length anyway.
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
    # Keep every length type, not just the TL/FL pairs coasts defaults to:
    # the SL conversions are the ones that matter most. See "Length types".
    length_types = NULL,
    strip_parentheticals = TRUE,
    # `conf` is what carries `metadata.fishbase.db_version`. Without it coasts
    # falls back to its own `read_config()` and resolves "latest", which is the
    # drift this key exists to stop.
    conf = conf
  )

  # Restated on a total-length basis by coasts, which logs what it converted.
  # Pairs with no usable conversion pass through carrying their original Type.
  fetched <- coasts::convert_lw_to_tl(m$length_weight, m$length_length)

  lw <- summarise_lw_coeffs(
    dplyr::bind_rows(fetched, curated_lw_coeffs())
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
#' A hand-curated table shipped with the package; edit the CSV directly.
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
#' The FishBase read is a live network read of a remote parquet dataset with no
#' pinned release, so a new FishBase release reaches the pipeline the moment a
#' container is rebuilt. Release **26.06** dissolved `Caesionidae` into
#' `Lutjanidae` and `Scaridae` into `Labridae` — both family names survive with
#' **zero species** — which took `CJX` and `PWT` to no coefficients at all.
#' Nothing failed, because a taxon with no coefficient pair produces `NA`
#' weight, which sums to zero — so a modelled taxon can vanish from the portal
#' silently.
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
      ". Every catch row of these taxa would weigh NA. Before changing any ",
      "code, check `metadata.fishbase.db_version`: a new FishBase release can ",
      "empty a family without removing its name. If the taxon's reference ",
      "name is a family, add a genus-level alias in `taxa_search_aliases()`."
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
      strip_parentheticals = TRUE,
      conf = conf
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
#' The **codes** come from the Airtable frame, the **names** from the FAO ASFIS
#' list keyed on `Alpha3_Code`. The two agree for 55 of Timor's 56 codes, the
#' exception being `MZZ`, and both expand to the same
#' 50 codes over the same species. ASFIS is used because the WIO pipelines use
#' it, not because it changes an answer.
#'
#' @param conf The configuration file.
#' @return A tibble with `alpha3_code` and `scientific_name`.
#' @keywords helper
#' @noRd
get_taxa_list <- function(conf) {
  codes <- get_assets(conf)$taxa %>%
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
#' No filtering on `Type` is applied, and since 2026-09-06 none is needed:
#' [get_morphometric_tables()] restates every convertible pair on a total-length
#' basis before calling this, so the pool is one measurement basis rather than
#' several. Filtering instead of converting would discard more than half the
#' matched species for `CJX`, `EMP`, `MOB` and `YDX`. Low-quality studies
#' (`EsQ == "yes"`) are dropped, as they were before.
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
