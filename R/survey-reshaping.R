#' Reshape KoBo species groups into one row per catch
#'
#' The landing forms record up to 26 catches per submission, each as a block of
#' `species_group.<n>.species_group/<field>` columns. This turns that block
#' structure into one row per (submission, catch), dropping the empty catch
#' slots.
#'
#' The catch index `n_catch` is 1-based, matching the other country pipelines.
#'
#' @param x A raw landings table, one row per submission.
#'
#' @return A tibble keyed on `submission_id` and `n_catch`, with one column per
#'   species-group field (`species`, `food_or_sale`, the
#'   `no_fish_by_length_group/*` counts, ...).
#' @keywords preprocessing
#' @export
#'
#' @examples
#' dummy <- tidyr::tibble(
#'   `_id` = "123",
#'   `species_group.0.species_group/species` = "1",
#'   `species_group.0.species_group/no_fish_by_length_group/no_individuals_5_10` = "3",
#'   `species_group.1.species_group/species` = NA_character_
#' )
#' reshape_species_groups(dummy)
reshape_species_groups <- function(x) {
  x %>%
    dplyr::select("_id", dplyr::starts_with("species_group")) %>%
    dplyr::mutate(dplyr::across(dplyr::everything(), as.character)) %>%
    # "species_group.3.species_group/no_fish_by_length_group/no_individuals_5_10"
    tidyr::pivot_longer(
      cols = -"_id",
      names_to = c(NA, "n_catch", "var"),
      names_sep = "\\."
    ) %>%
    tidyr::pivot_wider(names_from = "var", values_from = "value") %>%
    dplyr::rename_with(~ stringr::str_remove(.x, "^species_group/")) %>%
    dplyr::rename(submission_id = "_id") %>%
    # Empty catch slots carry no species. They are the padding that makes every
    # submission as wide as the widest one, not observations.
    dplyr::filter(!is.na(.data$species)) %>%
    dplyr::mutate(n_catch = as.integer(.data$n_catch) + 1L) %>%
    dplyr::arrange(.data$submission_id, .data$n_catch)
}

#' Expand each catch into one row per length bin
#'
#' The form records a count of individuals per 5 cm bin, so the length itself is
#' the **midpoint of the bin name**, not an answer. Individuals longer than
#' 60 cm are counted in `no_individuals_over60` and measured in
#' `fish_length_over60`; that recorded length replaces the 60 cm midpoint and
#' the measurement column is then dropped, which puts over-60 fish on the same
#' footing as every other bin.
#'
#' Bins with no count are **kept**. They carry a length and an `NA` count, they
#' are what the nested `length_frequency` column has always contained, and they
#' reach the portal: `timor_catch` is 1.7 M rows of which 93% have no weight.
#' Dropping them here would change the published table.
#'
#' @param x Output of [reshape_species_groups()].
#'
#' @return `x` with the `no_fish_by_length_group/*` columns replaced by one row
#'   per bin carrying `length` (cm) and `n_individuals`.
#' @keywords preprocessing
#' @export
expand_length_frequency <- function(x) {
  over_60 <- "no_fish_by_length_group/fish_length_over60"
  has_over_60 <- over_60 %in% names(x)

  long <- x %>%
    tidyr::pivot_longer(
      cols = dplyr::starts_with("no_fish_by_length_group"),
      names_to = "bin",
      values_to = "n_individuals"
    ) %>%
    dplyr::mutate(length = bin_midpoint(.data$bin))

  if (has_over_60) {
    long <- long %>%
      dplyr::group_by(.data$submission_id, .data$n_catch) %>%
      dplyr::mutate(
        length_over_60 = .data$n_individuals[.data$bin == over_60]
      ) %>%
      dplyr::ungroup() %>%
      dplyr::filter(.data$bin != over_60) %>%
      dplyr::mutate(
        length = dplyr::if_else(
          .data$length == 60 & !is.na(.data$length_over_60),
          as.numeric(.data$length_over_60),
          .data$length
        )
      ) %>%
      dplyr::select(-"length_over_60")
  }

  long %>%
    dplyr::mutate(n_individuals = as.numeric(.data$n_individuals)) %>%
    dplyr::arrange(.data$submission_id, .data$n_catch, .data$length) %>%
    dplyr::select(-"bin")
}

#' Midpoint of a KoBo length-bin column name
#'
#' `no_individuals_5_10` is 7.5 cm, `no_individuals_over60` is 60 cm. The
#' regex takes every run of digits preceded by `_` or by the `r` of "over",
#' which is what the pipeline has always done — it is reproduced rather than
#' rewritten so the bin midpoints cannot move.
#'
#' @param bin Character vector of bin column names.
#' @return A numeric vector of midpoints.
#' @keywords helper
#' @noRd
bin_midpoint <- function(bin) {
  levels <- unique(bin)
  midpoints <- purrr::map_dbl(
    stringr::str_extract_all(levels, "((?<=_|r)[0-9]+)"),
    ~ mean(as.numeric(.x))
  )
  midpoints[match(bin, levels)]
}

#' Strip stray newlines from free-text answers
#'
#' About 60 free-text answers carry leading or trailing newlines. They are
#' transcription noise, and `trimws()`'s default whitespace class removes
#' them.
#'
#' @param x A data frame.
#' @return `x` with every character column trimmed and `""` mapped to `NA`.
#' @keywords helper
#' @noRd
trim_free_text <- function(x) {
  dplyr::mutate(
    x,
    dplyr::across(
      tidyselect::where(is.character),
      ~ dplyr::na_if(trimws(.x), "")
    )
  )
}
