#' Download and read a versioned RDS object from public cloud storage
#'
#' Retrieves an `.rds` file from the configured public Google cloud storage using
#' an exact prefix match, downloads it locally, reads it into R, and deletes the
#' temporary local file. The function also attaches a `data_last_updated`
#' attribute parsed from the object filename (expected to contain a timestamp as
#' the 4th underscore-separated token in `%Y%m%d%H%M` format).
#'
#' @param prefix Character. Prefix used to locate the cloud object (exact match).
#'
#' @return The R object read from the RDS file, with an extra attribute
#'   `data_last_updated` (POSIXlt) indicating the timestamp extracted from the filename.
#'
#' @details
#' Requires configuration from `read_config()`. Uses `cloud_object_name()` to
#' resolve the object name and `download_cloud_file()` to download it. Assumes
#' filenames follow a convention like `..._<timestamp>_...` where `<timestamp>`
#' is in `%Y%m%d%H%M` format and located at position 4 after splitting on `_`.
#'
#' @seealso read_config, cloud_object_name, download_cloud_file
#' @export
#'
#' @keywords internal
#' @examples
#' \dontrun{
#' x <- get_file("timor_aggregated")
#' attr(x, "data_last_updated")
#' }
get_file <- function(prefix) {
  pars <- read_config()
  filename <- cloud_object_name(
    prefix = prefix,
    provider = pars$public_storage$google$key,
    extension = "rds",
    options = pars$public_storage$google$options,
    exact_match = TRUE
  )
  download_cloud_file(
    name = filename,
    provider = pars$public_storage$google$key,
    options = pars$public_storage$google$options
  )

  x <- readRDS(filename)
  file.remove(filename)
  date_modified <- strptime(
    strsplit(filename, "_")[[1]][4],
    format = "%Y%m%d%H%M"
  )

  attr(x, "data_last_updated") <- date_modified
  x
}

#' Format aggregated time-binned datasets for portal output
#'
#' Adds human-readable date labels (day/week/month/year) to aggregated datasets.
#' Supports two modes:
#' - Municipal mode (`municipal = TRUE`): expects a single data frame with
#'   `date_bin_start` and adds `month` and `year` columns.
#' - National mode (`municipal = FALSE`): expects a list of data frames
#'   (e.g., `day`, `week`, `month`, `year`), processes each data frame,
#'   sets `n_boats` to `national_boats`, and adds formatted date label columns.
#'
#' @param aggregated A data frame (municipal mode) or a list of data frames
#'   (national mode) containing a `date_bin_start` column.
#' @param municipal Logical. If `TRUE`, treat `aggregated` as a single municipal
#'   table; otherwise treat it as a list of time-binned tables.
#' @param national_boats Numeric/integer. Total number of boats to assign to the
#'   `n_boats` column in national mode.
#'
#' @return The formatted `aggregated` object: a data frame (municipal mode) or
#'   a list of data frames (national mode) with added/updated label columns.
#'
#' @details
#' In national mode, formatted labels are:
#' - `day`: `"%d %b %y"`
#' - `week`: `"%d %b %y"`
#' - `month`: `"%B %Y"` plus `year` as `"%Y"`
#' - `year`: `"%Y"`
#'
#' @seealso dplyr::mutate
#' @keywords internal
#' @export
#' @examples
#' \dontrun{
#' agg <- get_file("timor_aggregated")
#' agg2 <- format_aggregated_data(agg, national_boats = 123)
#'
#' m <- get_file("timor_municipal_aggregated")
#' m2 <- format_aggregated_data(m, municipal = TRUE)
#' }
format_aggregated_data <- function(
  aggregated,
  municipal = FALSE,
  national_boats = NULL
) {
  if (isTRUE(municipal)) {
    aggregated <- aggregated %>%
      dplyr::mutate(
        month = format(.data$date_bin_start, format = "%B %Y"),
        year = format(.data$date_bin_start, format = "%Y")
      )
  } else {
    aggregated <- lapply(aggregated, function(x) {
      x %>%
        dplyr::mutate(
          n_boats = national_boats
        )
    })
    aggregated$day <- aggregated$day %>%
      dplyr::mutate(day = format(.data$date_bin_start, format = "%d %b %y"))
    aggregated$week <- aggregated$week %>%
      dplyr::mutate(week = format(.data$date_bin_start, format = "%d %b %y"))
    aggregated$month <- aggregated$month %>%
      dplyr::mutate(
        month = format(.data$date_bin_start, format = "%B %Y"),
        year = format(.data$date_bin_start, format = "%Y")
      )
    aggregated$year <- aggregated$year %>%
      dplyr::mutate(year = format(.data$date_bin_start, format = "%Y"))
  }
  aggregated
}

#' Build a lookup list of taxa names grouped by fish group
#'
#' From an input table containing `catch_taxon` and `fish_group`, constructs a
#' named list where each element corresponds to one `fish_group` and contains
#' the unique taxa (from `catch_taxon`) observed in that group.
#'
#' @param x A data frame with at least columns `catch_taxon` and
#'   `fish_group`.
#'
#' @return A named list. Names are fish group labels; values are lists of unique
#'   taxa strings belonging to each group.
#'
#' @details
#' Internally creates a tibble with unique taxa within group, and
#' uses `split()` to produce the group-wise list.
#'
#' @seealso dplyr::tibble, split
#' @keywords internal
#' @export
#' @examples
#' \dontrun{
#' grid <- get_file("indicators_gridded")
#' groups <- label_taxa_groups(grid)
#' names(groups)
#' groups[["Small pelagics"]]
#' }
label_taxa_groups <- function(x) {
  label_groups <-
    dplyr::tibble(
      taxa = x$catch_taxon,
      group = x$fish_group
    ) |>
    dplyr::distinct(.data$group, .data$taxa)

  label_groups_list <- split(label_groups$taxa, label_groups$group)

  # make each element a list of 1-length character vectors
  label_groups_list <- lapply(label_groups_list, as.list)

  label_groups_list
}

#' Rename fields to match the portal ontology
#'
#' Standardizes column names by applying a set of pattern-based substitutions:
#' - `catch_price` -> `revenue`
#' - `municipality` -> `region`
#' - `landing_catch` -> `landing_weight`
#' - `catch_preservation` -> `conservation_place`
#' - `gear` -> `gear_type`
#'
#' @param x A data frame-like object with named columns.
#'
#' @return The same object `x`, with updated column names.
#'
#' @details
#' Uses `gsub()` over `names(x)`; substitutions are applied sequentially.
#' Note that replacing `gear` with `gear_type` is pattern-based and will also
#' affect any column name containing the substring `"gear"`.
#' @keywords internal
#' @export
#' @examples
#' \dontrun{
#' df <- rename_ontology(df)
#' }
rename_ontology <- function(x) {
  names(x) <- gsub("catch_price", "revenue", names(x))
  names(x) <- gsub("municipality", "region", names(x))
  names(x) <- gsub("landing_catch", "landing_weight", names(x))
  names(x) <- gsub("catch_preservation", "conservation_place", names(x))
  names(x) <- gsub("gear", "gear_type", names(x))
  return(x)
}

#' Export portal datasets as versioned JSON files to public cloud storage
#'
#' Orchestrates the portal data export workflow:
#' 1) Downloads multiple precomputed objects from public cloud storage via
#'    `get_file()`.
#' 2) Standardizes column names via `rename_ontology()`.
#' 3) Formats time-binned tables via `format_aggregated_data()`.
#' 4) Builds taxa group lookup via `label_taxa_groups()`.
#' 5) Computes summary tables (`estimated_tons`, `estimated_revenue`) and a
#'    curated `summary_data` list for portal use.
#' 6) Writes each object to pretty-printed JSON with versioned filenames and
#'    uploads them to public cloud storage.
#'
#' @return Invisibly returns `NULL`. Called for its side effects (JSON creation
#'   and upload).
#'
#' @details
#' Exported JSON objects include (at minimum):
#' `aggregated`, `taxa_aggregated`, `municipal_aggregated`, `municipal_taxa`,
#' `nutrients_aggregated`, `data_last_updated`, `indicators_grid`,
#' `label_groups_list`, and `summary_data`.
#'
#' The function expects `summary_data` (downloaded) to contain fields such as
#' `n_surveys`, `catch_norm`, `catch_price_norm`, `nutrients_per_catch`,
#' `nutrients_norm`, `conservation`, `cpue_df`, and `timor_shape`.
#'
#' Assumes a configured public Google cloud storage provider in
#' `pars$public_storage$google` and relies on helper functions such as
#' `add_version()`, `upload_cloud_file()`, and JSON serialization via `toJSON()`.
#'
#' @seealso get_file, rename_ontology, format_aggregated_data, label_taxa_groups,
#'   upload_cloud_file, add_version
#'
#' @keywords workflow
#' @export
#' @examples
#' \dontrun{
#' export_files()
#' }
export_files <- function() {
  pars <- read_config()
  aggregated <- get_file("timor_aggregated")
  data_last_updated <- attr(aggregated, "data_last_updated")
  aggregated <- aggregated %>% purrr::map(rename_ontology)
  municipal_aggregated <- get_file("timor_municipal_aggregated") %>%
    rename_ontology()
  taxa_aggregated <- get_file("timor_taxa_aggregated") %>%
    purrr::map(rename_ontology)
  municipal_taxa <- get_file("timor_municipal_taxa") %>% rename_ontology()
  nutrients_aggregated <- get_file("timor_nutrients_aggregated") %>%
    purrr::map(., ~ dplyr::filter(.x, !nutrient == "selenium"))
  summary_data <- get_file("summary_data")

  indicators_grid <- get_file("indicators_gridded")
  label_groups_list <- label_taxa_groups(indicators_grid)

  boats <- sum(unique(municipal_aggregated$n_boats))

  aggregated <- format_aggregated_data(aggregated, national_boats = boats)
  municipal_aggregated <- format_aggregated_data(
    municipal_aggregated,
    municipal = TRUE
  )
  taxa_aggregated <- format_aggregated_data(
    taxa_aggregated,
    national_boats = boats
  )
  nutrients_aggregated <- format_aggregated_data(
    nutrients_aggregated,
    national_boats = boats
  )
  municipal_taxa <- format_aggregated_data(municipal_taxa, municipal = TRUE)

  estimated_tons <-
    taxa_aggregated$month %>%
    dplyr::mutate(
      fish_group = dplyr::case_when(
        grouped_taxa %in% c("COZ") ~ "Molluscs",
        grouped_taxa %in% c("PEZ") ~ "Shrimps",
        grouped_taxa %in% c("MZZ") ~ "Other",
        grouped_taxa %in% c("SLV", "CRA") ~ "Crustaceans",
        grouped_taxa %in% c("OCZ", "IAX") ~ "Cephalopods",
        grouped_taxa %in% c("SKH", "SRX") ~ "Sharks and rays",
        grouped_taxa %in% c("SNA", "GPX", "PWT", "GRX", "MUI", "BGX") ~
          "Large demersals",
        grouped_taxa %in%
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
          ) ~
          "Large pelagics",
        grouped_taxa %in%
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
          ) ~
          "Small demersals",
        grouped_taxa %in%
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
          ) ~
          "Small pelagics",
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::group_by(.data$fish_group) %>%
    dplyr::summarise(tons = sum(.data$catch, na.rm = T) / 1000) %>%
    dplyr::mutate(tons = round(.data$tons, 0)) %>%
    dplyr::arrange(-.data$tons)

  estimated_revenue <-
    municipal_aggregated %>%
    dplyr::mutate(
      Area = dplyr::case_when(
        .data$region %in%
          c("Oecusse", "Bobonaro", "Liquica", "Dili", "Manatuto", "Baucau") ~
          "North Coast",
        .data$region == "Atauro" ~ "Atauro island",
        TRUE ~ "South Coast"
      )
    ) %>%
    dplyr::group_by(.data$Area) %>%
    dplyr::summarise(`Estimated revenue` = sum(.data$revenue, na.rm = T)) %>%
    dplyr::mutate(`Estimated revenue` = round(`Estimated revenue`, 0))

  summary_data <-
    list(
      n_surveys = summary_data$n_surveys,
      estimated_tons = estimated_tons,
      estimated_revenue = estimated_revenue,
      catch_habitat = summary_data$catch_norm,
      revenue_habitat = summary_data$catch_price_norm,
      nutrients_per_catch = summary_data$nutrients_per_catch,
      nutrients_habitat = summary_data$nutrients_norm,
      conservation = summary_data$conservation,
      region_cpue = summary_data$cpue_df,
      timor_boundaries = summary_data$timor_shape
    )

  write_json <- function(x, path) {
    writeLines(
      jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", pretty = TRUE),
      path
    )
  }

  objs <- list(
    aggregated = aggregated,
    taxa_aggregated = taxa_aggregated,
    municipal_aggregated = municipal_aggregated,
    municipal_taxa = municipal_taxa,
    nutrients_aggregated = nutrients_aggregated,
    data_last_updated = data_last_updated,
    indicators_grid = indicators_grid,
    label_groups_list = label_groups_list,
    summary_data = summary_data
  )

  tmp_dir <- file.path(tempdir(), "portal-json")
  dir.create(tmp_dir, showWarnings = FALSE, recursive = TRUE)

  files <- purrr::imap_chr(names(objs), \(nm, ...) {
    file.path(tmp_dir, add_version(paste0("portal-", nm), extension = "json"))
  })

  purrr::walk2(objs, files, write_json)

  purrr::walk(
    files,
    upload_cloud_file,
    provider = pars$public_storage$google$key,
    options = pars$public_storage$google$options
  )
}
