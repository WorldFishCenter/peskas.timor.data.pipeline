# ---------------------------------------------------------------------------
# THE SHAPE
# Every validator in this file reads the **flat long** weighted catch table of
# [calculate_weights()] — one row per (submission, catch, length bin) — or the
# one-row-per-submission view [validation_submissions()] derives from it, and
# every one addresses columns by their standard names.
#
# THE ALERT CODES
# The codes are the contract — the flag strings they compose are published and
# the shared validation UI keys off them. `inst/config.yml`'s
# `validation.alerts` block is the authoritative description of each.
#
#   code             validator                       meaning
#   1, 2, 3   [validate_imeis()]                IMEI too short / ambiguous / unknown
#   4, 10     [validate_surveys_time()]         landing after submission / submitted too late
#   5         [validate_surveys_time()]         implausible trip duration
#   6         [validate_catch_price()]          revenue too large
#   11        [validate_catch_params()]         individuals per catch an outlier
#   12, 13    [validate_vessel_type()]          unknown vessel code / missing vessel
#   14, 15    [validate_gear_type()]            unknown gear code / missing gear
#   16        [validate_sites()]                landing site not known
#   17        [validate_price_weight()]         price per kg outside the configured band
#   18        [validate_n_fishers()]            crew size an outlier
#   19        [validate_habitat()]              unknown habitat code
#   20        [validate_mesh()]                 implausible mesh size
#   21        [validate_gleaners()]             gleaner count an outlier
#   22        [validate_landing_regularity()]   catch, revenue and individuals disagree
#   23        [validate_fuel()]                 implausible fuel, or fuel without a boat
#
# Codes 23 (fuel), and the conservation and happiness frames, feed the
# validated artefact but deliberately **not** the flag string — see
# [validate_landings()].
#
# ---------------------------------------------------------------------------

# The submission-level columns every validator works from. Constant within a
# submission by construction, so `distinct()` yields exactly one row each.
validation_submission_cols <- function() {
  c(
    "submission_id", "survey_version", "survey_id", "submitted_by",
    "landing_date", "submission_date", "trip_duration", "tracker_imei",
    "catch_price", "landing_site_code", "gear_code", "gear", "vessel_code",
    "vessel_type", "habitat_code", "habitat", "has_boat", "mesh_size",
    "n_gleaners", "fuel", "conservation_code", "happiness",
    "no_men_fishers", "no_women_fishers", "no_child_fishers"
  )
}

# The catch-level measurements a raised alert blanks: the estimated weight and
# the nutrient masses derived from it.
catch_value_cols <- function() {
  c("weight", "Selenium_mu", "Zinc_mu", "Protein_mu", "Omega_3_mu",
    "Calcium_mu", "Iron_mu", "Vitamin_A_mu")
}

#' One row per submission
#'
#' Collapses the long weighted catch table to its submission-level columns.
#'
#' `submitted_by` is set to the reporting region, not the KoBo account. One
#' enumerator covers each of the twelve reporting regions — Atauro has its own,
#' separate from the rest of Dili — but they share logins: 10 accounts cover
#' 97,753 submissions and one of them carries 87% of those across every
#' municipality, so the account name does not identify who collected a landing.
#' The region does, which is why this uses [frame_reporting_region()] rather
#' than `gaul_1_name`: that would fold Atauro's 39,968 submissions into Dili's
#' and merge two enumerators into one.
#'
#' Where no region resolves the account name is kept, and the column keeps its
#' name because the validation app reads it by that name.
#'
#' @param landings The long weighted catch table from [get_weighted_landings()].
#' @return A tibble with one row per submission.
#' @keywords validation
#' @export
validation_submissions <- function(landings) {
  landings %>%
    dplyr::select(dplyr::all_of(
      c(validation_submission_cols(), "gaul_1_name", "gaul_2_name")
    )) %>%
    dplyr::distinct() %>%
    frame_reporting_region() %>%
    dplyr::mutate(
      submission_id = as.integer(.data$submission_id),
      submitted_by = dplyr::coalesce(
        .data$reporting_region, .data$submitted_by
      )
    ) %>%
    dplyr::select(dplyr::all_of(validation_submission_cols()))
}

#' Generate an alert vector based on the `univOutl::LocScaleB()` function
#'
#' @param x numeric vector where outliers will be checked
#' @param no_alert_value value to put in the output when there is no alert (x is within bounds)
#' @param alert_if_larger alert for when x is above the bounds found by `univOutl::LocScaleB()`
#' @param alert_if_smaller alert for when x is below the bounds found by `univOutl::LocScaleB()`
#' @param ... arguments for `univOutl::LocScaleB()`
#'
#' @return a vector of the same lenght as x
#' @keywords internal
#' @importFrom stats mad
alert_outlier <- function(x,
                          no_alert_value = NA_real_,
                          alert_if_larger = no_alert_value,
                          alert_if_smaller = no_alert_value,
                          ...) {
  algo_args <- list(...)

  # Helper function to check if everything is NA or zero
  all_na_or_zero <- function(x) {
    isTRUE(all(is.na(x) | x == 0))
  }

  # If everything is NA or zero there is nothing to compute
  if (all_na_or_zero(x)) {
    return(NA_real_)
  }
  # If the median absolute deviation is zero we shouldn't be using this algo
  if (mad(x, na.rm = T) <= 0) {
    return(NA_real_)
  }
  # If weights are specified and they are all NA or zero
  if (!is.null(algo_args$weights)) {
    if (all_na_or_zero(algo_args$weights)) {
      return(NA_real_)
    }
  }

  bounds <- univOutl::LocScaleB(x, ...) %>%
    magrittr::extract2("bounds")

  if (isTRUE(algo_args$logt)) bounds <- exp(bounds) - 1

  dplyr::case_when(
    x < bounds[1] ~ alert_if_smaller,
    x > bounds[2] ~ alert_if_larger,
    TRUE ~ no_alert_value
  )
}

# Perform tests for a single imei and return the corrected value and the flag
validate_this_imei <- function(this_imei, this_id = NULL, valid_imeis) {
  this_id <- as.integer(this_id)

  # If imei is NA there is nothing to validate
  if (is.na(this_imei)) {
    out <- list(imei = NA_character_, alert_number = NA_integer_, submission_id = this_id)
    return(out)
  }

  # Zero seems to be used for no IMEI as well
  if (this_imei == "0") {
    out <- list(imei = NA_character_, alert_number = NA_integer_, submission_id = this_id)
    return(out)
  }

  # If the IMEI is negative it was probably a typo
  this_imei <- as.numeric(this_imei)
  if (this_imei < 0) this_imei <- this_imei * -1

  # Optimistically we need at least 5 digits to work with and that might be
  if (this_imei < 9999) {
    out <- list(imei = NA_character_, alert_number = 1, submission_id = this_id)
    return(out)
  }

  # If a valid IMEI is found replace it
  imei_regex <- paste0(as.character(this_imei), "$")
  imei_matches <- stringr::str_detect(valid_imeis, imei_regex)
  n_matches <- sum(imei_matches)
  if (n_matches == 1) {
    list(imei = valid_imeis[imei_matches], alert_number = NA_integer_, submission_id = this_id)
  } else if (n_matches > 1) {
    list(imei = NA_character_, alert_number = 2, submission_id = this_id)
  } else if (n_matches == 0) {
    list(imei = NA_character_, alert_number = 3, submission_id = this_id)
  }
}

#' Validate the tracker IMEIs recorded on a survey
#'
#' Enumerators record as many digits of the tracker IMEI as they can read, so
#' a recorded value is matched against the deployed devices by **suffix**. A
#' value short enough to be ambiguous, or matching no device, is flagged.
#'
#' The roster spans every device ever deployed, since a submission from any year
#' can reference one.
#'
#' @param submissions Output of [validation_submissions()].
#' @param deployed_imeis Character vector of deployed device IMEIs.
#' @return A tibble: `imei`, `alert_number`, `submission_id`.
#' @keywords validation
#' @export
validate_imeis <- function(submissions, deployed_imeis) {
  submissions$tracker_imei %>%
    rlang::set_names(submissions$submission_id) %>%
    purrr::imap(validate_this_imei, deployed_imeis) %>%
    purrr::map_dfr(tibble::as_tibble)
}

#' Validate surveys' temporal parameters
#'
#' This function takes the submission-level view of the landings and validates
#' the temporal info associated to each survey.
#'
#' @param submissions Output of [validation_submissions()].
#' @param hrs Limit of trip duration in hours to be considered a valid catch
#'   session.
#' @param submission_delay Limit for maximum difference (in days) between the
#'   survey submission date and the recorded landing date.
#'
#' @return A list containing data frames with validated catch dates and catch
#'   duration.
#'
#' @importFrom rlang .data
#' @keywords validation
#' @export
#'
#' @examples
#' \dontrun{
#' conf <- read_config()
#' landings <- get_weighted_landings(conf)
#' validate_surveys_time(validation_submissions(landings), hrs = 18, 28)
#' }
validate_surveys_time <- function(submissions, hrs = NULL, submission_delay) {
  list(
    validated_dates = submissions %>%
      dplyr::select("submission_id", "landing_date", "submission_date") %>%
      dplyr::transmute(
        # Alert needs to be checked before editing the date column. The landing
        # date is a calendar date; compare it at local midnight, as the raw
        # character date always was.
        date = as.POSIXct(as.character(.data$landing_date), tz = "Asia/Dili"),
        alert_number = dplyr::case_when(
          # test if submission date is prior catch date
          .data$date > .data$submission_date ~ 4,
          .data$date <
            .data$submission_date -
              lubridate::duration(submission_delay, units = "days") ~ 10,
          TRUE ~ NA_real_
        ),
        date = as.Date(.data$date, tz = "Asia/Dili"),
        submission_id = .data$submission_id
      ) %>%
      dplyr::select("alert_number", "date", "submission_id"),
    validated_duration = submissions %>%
      dplyr::transmute(
        duration = abs(.data$trip_duration),
        # test if catch duration is longer than n hours or minor than 1 hour
        trip_length = dplyr::if_else(
          .data$duration > hrs | .data$duration < 1, NA_real_, .data$duration
        ),
        alert_number = dplyr::if_else(
          .data$duration > hrs | .data$duration < 1, 5, NA_real_
        ),
        submission_id = .data$submission_id
      ) %>%
      dplyr::select("trip_length", "alert_number", "submission_id")
  )
}

#' Validate the internal consistency of a landing
#'
#' A landing must agree with itself: a recorded no-catch cannot carry
#' individuals or revenue, and a recorded catch cannot lack both.
#'
#' @param landings The long weighted catch table.
#' @return A list with `regularity_alerts` (one row per submission) and
#'   `regular_landings` (the long table, with the offending rows' measurements
#'   blanked and `alert_number` set).
#' @keywords validation
#' @export
validate_landing_regularity <- function(landings) {
  regularity_alerts <-
    landings %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::summarise(
      catch_taxon = dplyr::first(.data$catch_taxon),
      catch_price = abs(dplyr::first(.data$catch_price)),
      n_individuals = sum(abs(.data$n_individuals), na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      alert_regularity = dplyr::case_when(
        .data$catch_taxon == "0" & .data$n_individuals > 0 |
          .data$catch_taxon == "0" & .data$catch_price > 0 |
          !.data$catch_taxon == "0" & .data$n_individuals <= 0 |
          !.data$catch_taxon == "0" & .data$catch_price <= 0 |
          .data$catch_price <= 0 & .data$n_individuals > 0 |
          .data$catch_price > 0 & .data$n_individuals <= 0
        ~ 22, TRUE ~ NA_real_
      ),
      submission_id = as.integer(.data$submission_id)
    )

  no_regular_ids <-
    regularity_alerts %>%
    dplyr::filter(!is.na(.data$alert_regularity)) %>%
    magrittr::extract2("submission_id")

  regular_landings <-
    landings %>%
    dplyr::mutate(
      submission_id = as.integer(.data$submission_id),
      catch_price = abs(.data$catch_price),
      n_individuals = abs(.data$n_individuals),
      alert_number = dplyr::if_else(
        .data$submission_id %in% no_regular_ids, 22, NA_real_
      )
    ) %>%
    blank_on_alert(c("catch_price", "n_individuals", catch_value_cols()))

  list(
    regularity_alerts = regularity_alerts,
    regular_landings = regular_landings
  )
}

# A raised alert invalidates the measurements it was raised about. Every
# validator that flags a row also blanks it, and always the same way.
blank_on_alert <- function(x, cols) {
  dplyr::mutate(
    x,
    dplyr::across(
      dplyr::all_of(cols),
      ~ dplyr::if_else(is.na(.data$alert_number), .x, NA_real_)
    )
  )
}

#' Validate surveys' total catch values
#'
#' Flags implausibly large revenue. The `univOutl::LocScaleB()` route is kept in
#' place, commented out, because the fixed threshold replaced it deliberately.
#'
#' @param regular_landings The long catch table from
#'   [validate_landing_regularity()], carrying `catch_price` and the
#'   `alert_number` raised so far.
#' @inheritParams univOutl::LocScaleB
#'
#' @return A data frame containing validated catch values, one row per
#'   submission.
#' @keywords validation
#' @export
#'
#' @importFrom rlang .data
validate_catch_price <- function(regular_landings, method = NULL, k = NULL) {
  # `catch_price` and `alert_number` are submission-level in this frame.
  submissions <- dplyr::distinct(
    regular_landings,
    .data$submission_id, .data$catch_price, .data$alert_number
  )

  validated_price <-
    submissions %>%
    dplyr::filter(is.na(.data$alert_number)) %>%
    dplyr::transmute(
      alert_number = ifelse(.data$catch_price > 1500, 6, NA_integer_),
      # alert_number = alert_outlier(
      #  x = .data$catch_price, alert_if_smaller = 9, alert_if_larger = 6,
      #  logt = TRUE, k = k, method = method
      # ),
      catch_price = dplyr::if_else(
        is.na(.data$alert_number), .data$catch_price, NA_real_
      ),
      submission_id = .data$submission_id
    )

  submissions %>%
    dplyr::filter(!is.na(.data$alert_number)) %>%
    dplyr::select("submission_id", "catch_price", "alert_number") %>%
    dplyr::bind_rows(validated_price)
}

#' Validate surveys' catch parameters
#'
#' Uses univariate techniques (see [univOutl::LocScaleB]) to identify outliers
#' in the number of individuals recorded per (gear, taxon) group, and blanks the
#' weight and nutrients derived from an outlying count.
#'
#' Grouping is on the resolved `gear` label rather than the raw gear code; the
#' two are one-to-one over all nine codes the live forms use, so the partition
#' is identical.
#'
#' @param landings The long catch table from [validate_landing_regularity()],
#'   carrying `alert_number`.
#' @param k_ind Extension of bounds for the number of individuals
#'
#' @return A list with `alerts` (one row per submission) and `catch` (the long
#'   table with outlying counts and their weights blanked).
#' @keywords validation
#' @export
#'
#' @examples
#' \dontrun{
#' conf <- read_config()
#' landings <- get_weighted_landings(conf)
#' validate_catch_params(validate_landing_regularity(landings)$regular_landings)
#' }
validate_catch_params <- function(landings = NULL, k_ind = NULL) {
  already_alerted <- dplyr::filter(landings, !is.na(.data$alert_number))

  validated <-
    landings %>%
    dplyr::filter(is.na(.data$alert_number)) %>%
    dplyr::group_by(.data$gear, .data$catch_taxon) %>%
    dplyr::mutate(
      alert_number = alert_outlier(
        x = .data$n_individuals,
        alert_if_larger = 11, logt = TRUE, k = k_ind
      ),
      n_individuals = dplyr::if_else(
        is.na(.data$alert_number), .data$n_individuals, NA_real_
      )
    ) %>%
    dplyr::ungroup() %>%
    # Adjusting weight and nutrients accordingly: a counted zero weighs zero,
    # an outlying count weighs nothing we can trust.
    dplyr::mutate(
      dplyr::across(
        dplyr::all_of(catch_value_cols()),
        ~ dplyr::case_when(
          !is.na(.data$alert_number) ~ NA_real_,
          .data$n_individuals == 0 ~ 0,
          TRUE ~ .x
        )
      )
    )

  # One alert per submission: the worst raised across its catches.
  alerts <-
    validated %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::summarise(
      alert_number = if (all(is.na(.data$alert_number))) {
        NA_real_
      } else {
        max(.data$alert_number, na.rm = TRUE)
      },
      .groups = "drop"
    )

  list(
    alerts = dplyr::bind_rows(
      already_alerted %>%
        dplyr::distinct(.data$submission_id, .data$alert_number),
      alerts
    ),
    catch = dplyr::bind_rows(already_alerted, validated)
  )
}

#' Outlier identification of the price-weight relation
#'
#' Flags submissions whose revenue per kilo of estimated catch falls outside the
#' configured band, and propagates the regularity, revenue and
#' individuals-count alerts onto both the price and the catch frames so that a
#' single alert number per submission survives the assembly.
#'
#' The `cook_dist` parameter is unused and retained only because `config.yml`
#' still supplies it.
#'
#' @param catch_params Output of [validate_catch_params()]: `alerts`
#'   (one row per submission) and `catch` (the long table).
#' @param price_alerts The data frame of price alerts.
#' @param non_regular_ids The data frame of landings regularity alerts.
#' @param cook_dist Unused. See above.
#' @param price_weight_min Min price per weight value threshold.
#' @param price_weight_max Max price per weight value threshold.
#' @return A list with `alerts` (one row per submission, carrying the validated
#'   `catch_price`) and `catch` (the long table with blanked measurements).
#' @keywords validation
#' @export
#'
validate_price_weight <- function(catch_params = NULL,
                                  price_alerts = NULL,
                                  non_regular_ids = NULL,
                                  cook_dist = NULL,
                                  price_weight_min = NULL,
                                  price_weight_max = NULL) {
  # Extract IDs with an abnormal price per kilo of estimated catch
  price_per_weight_alerts <-
    catch_params$catch %>%
    dplyr::select("submission_id", "weight") %>%
    dplyr::left_join(
      dplyr::select(price_alerts, "submission_id", "catch_price"),
      by = "submission_id"
    ) %>%
    dplyr::filter(
      !is.na(.data$weight), !is.na(.data$catch_price), .data$weight != 0
    ) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::summarise(
      catch_price = dplyr::first(.data$catch_price),
      weight = sum(.data$weight, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::mutate(pk = .data$catch_price / (.data$weight / 1000)) %>%
    dplyr::filter(.data$pk < price_weight_min | .data$pk > price_weight_max) %>%
    magrittr::extract2("submission_id")

  regularity_alerts <-
    non_regular_ids %>%
    dplyr::filter(!is.na(.data$alert_regularity)) %>%
    magrittr::extract2("submission_id")

  n_individuals_alert <-
    catch_params$alerts %>%
    dplyr::filter(!is.na(.data$alert_number)) %>%
    magrittr::extract2("submission_id")

  revenue_alert <-
    price_alerts %>%
    dplyr::filter(!is.na(.data$alert_number)) %>%
    magrittr::extract2("submission_id")

  # Integrate the new alert into prices and into the catches.
  #
  # Note the scope: this pass blanks a submission's catch measurements from its
  # **submission-level** alert, so a single outlying individuals count (alert 11)
  # discards every catch of that landing, not merely the outlying one. That is
  # the long-standing behaviour — `validate_catch_params()` used to return one
  # nested row per submission, so un-nesting it spread the alert over every
  # row — and it is worth 845 submissions and 1.44% of validated catch weight.
  # Narrowing it to the offending rows is defensible and easy (drop the join
  # below and keep `catch_params$catch`'s own row-level `alert_number`), but it
  # would change published aggregates, so it is a decision rather than a side
  # effect of this phase.
  propagate_catch <- function(x) {
    dplyr::mutate(
      x,
      alert_number = dplyr::case_when(
        .data$submission_id %in% price_per_weight_alerts ~ 17,
        .data$submission_id %in% regularity_alerts ~ 22,
        .data$submission_id %in% revenue_alert ~ 6,
        TRUE ~ .data$alert_number
      )
    )
  }

  price_alerts <-
    price_alerts %>%
    dplyr::mutate(
      alert_number = dplyr::case_when(
        .data$submission_id %in% price_per_weight_alerts ~ 17,
        .data$submission_id %in% regularity_alerts ~ 22,
        .data$submission_id %in% n_individuals_alert ~ 11,
        TRUE ~ .data$alert_number
      )
    ) %>%
    blank_on_alert("catch_price")

  catch_alerts <- propagate_catch(catch_params$alerts)

  list(
    alerts = dplyr::full_join(
      catch_alerts,
      price_alerts %>%
        dplyr::select("submission_id", "catch_price", "alert_number"),
      by = "submission_id"
    ) %>%
      dplyr::mutate(
        alert_number = dplyr::coalesce(
          .data$alert_number.x, .data$alert_number.y
        )
      ) %>%
      dplyr::select(-"alert_number.x", -"alert_number.y"),
    catch = catch_params$catch %>%
      dplyr::select(-"alert_number") %>%
      dplyr::left_join(catch_alerts, by = "submission_id") %>%
      blank_on_alert(c("n_individuals", catch_value_cols()))
  )
}

#' Validate the recorded vessel type
#'
#' Reads the `vessel_type` label [preprocess_landings()] resolves from the
#' Airtable frame. Both codes the live forms use resolve, so alert 12 is
#' currently never raised.
#'
#' @param submissions Output of [validation_submissions()].
#' @return A tibble: `vessel_type`, `alert_number`, `submission_id`.
#' @keywords validation
#' @export
validate_vessel_type <- function(submissions) {
  submissions %>%
    dplyr::mutate(
      alert_number = dplyr::case_when(
        # A code was recorded that the frame does not know
        !is.na(.data$vessel_code) & is.na(.data$vessel_type) ~ 12,
        # A boat was declared but its type was never recorded
        .data$has_boat == "TRUE" & is.na(.data$vessel_type) ~ 13,
        TRUE ~ NA_real_
      ),
      vessel_type = dplyr::if_else(
        is.na(.data$alert_number), .data$vessel_type, NA_character_
      )
    ) %>%
    dplyr::select("vessel_type", "alert_number", "submission_id")
}

#' Validate the recorded gear type
#'
#' Reads the `gear` label [preprocess_landings()] resolves from the
#' PESKAS | FRAME frame. Note the frame's labels are Title Case where the
#' older reference table was lower case, and `MC` is
#' "Gleaning" where the Sheets said "manual collection".
#'
#' @param submissions Output of [validation_submissions()].
#' @return A tibble: `gear_type`, `alert_number`, `submission_id`.
#' @keywords validation
#' @export
validate_gear_type <- function(submissions) {
  submissions %>%
    dplyr::mutate(
      alert_number = dplyr::case_when(
        !is.na(.data$gear_code) & is.na(.data$gear) ~ 14,
        .data$has_boat == "TRUE" & is.na(.data$gear_code) ~ 15,
        TRUE ~ NA_real_
      ),
      gear_type = dplyr::if_else(
        is.na(.data$alert_number), .data$gear, NA_character_
      )
    ) %>%
    dplyr::select("gear_type", "alert_number", "submission_id")
}

#' Validate the recorded landing site
#'
#' Resolves the site name and reporting region from the Airtable frame by site
#' code. Alert 16 fires when the recorded code matches no known site.
#'
#' @param submissions Output of [validation_submissions()].
#' @param frame_sites The frame `sites` table from [get_assets()].
#' @param frame_geo The frame `geo` table from [get_assets()].
#' @return A tibble: `submission_id`, `station_code`, `station_name`,
#'   `reporting_region`, `alert_number`.
#' @keywords validation
#' @export
validate_sites <- function(submissions,
                           frame_sites,
                           frame_geo) {
  regions <-
    frame_geo %>%
    frame_reporting_region() %>%
    dplyr::transmute(
      gaul_2_code = as.character(.data$gaul_2_code),
      .data$reporting_region
    ) %>%
    dplyr::distinct()

  sites_df <-
    frame_sites %>%
    dplyr::filter(!is.na(.data$site_code)) %>%
    dplyr::transmute(
      station_code = as.character(.data$site_code),
      # One frame site name carries an embedded newline, and these are
      # published labels -- squish rather than merely trim.
      station_name = stringr::str_squish(.data$site),
      gaul_2_code = as.character(.data$gaul_2_code)
    ) %>%
    dplyr::left_join(regions, by = "gaul_2_code") %>%
    dplyr::select("station_code", "station_name", "reporting_region") %>%
    dplyr::distinct()

  submissions %>%
    dplyr::select("submission_id", station_code = "landing_site_code") %>%
    dplyr::left_join(sites_df, by = "station_code") %>%
    # If the station is not known to us
    dplyr::mutate(
      alert_number = dplyr::if_else(
        is.na(.data$station_name) | is.na(.data$reporting_region), 16, NA_real_
      )
    )
}

#' Validate the recorded crew size
#'
#' @param submissions Output of [validation_submissions()].
#' @inheritParams univOutl::LocScaleB
#' @return A tibble: `submission_id` and the three `fisher_number_*` columns.
#' @keywords validation
#' @export
validate_n_fishers <- function(submissions, method, k) {
  submissions %>%
    dplyr::select(
      "submission_id",
      fisher_number_child = "no_child_fishers",
      fisher_number_man = "no_men_fishers",
      fisher_number_woman = "no_women_fishers"
    ) %>%
    dplyr::mutate(dplyr::across(
      tidyselect::starts_with("fisher"),
      list(alert = alert_outlier),
      alert_if_larger = 18, alert_if_smaller = 18, k = k, logt = T,
      method = method
    )) %>%
    dplyr::mutate(alert_number = dplyr::coalesce(
      .data$fisher_number_child_alert,
      .data$fisher_number_man_alert,
      .data$fisher_number_woman_alert
    )) %>%
    dplyr::mutate(dplyr::across(
      tidyselect::starts_with("fisher"),
      ~ dplyr::if_else(!is.na(.data$alert_number), NA_real_, .)
    )) %>%
    dplyr::select(-tidyselect::ends_with("alert"))
}

#' Validate the recorded habitat
#'
#' Habitat is the one label with no frame equivalent, so
#' [preprocess_landings()] resolves it from a fixed code-to-label lookup.
#' This validator therefore only has to notice a code the lookup does not
#' cover.
#'
#' @param submissions Output of [validation_submissions()].
#' @return A tibble: `submission_id`, `habitat_code`, `habitat_type`,
#'   `alert_number`.
#' @keywords validation
#' @export
validate_habitat <- function(submissions) {
  submissions %>%
    dplyr::transmute(
      .data$submission_id,
      .data$habitat_code,
      habitat_type = .data$habitat,
      alert_number = dplyr::if_else(
        !is.na(.data$habitat_code) & is.na(.data$habitat), 19, NA_real_
      ),
      habitat_type = dplyr::if_else(
        is.na(.data$alert_number), .data$habitat_type, NA_character_
      )
    )
}

#' Validate the recorded mesh size
#'
#' `mesh_size` reaches this function in millimetres, converted by
#' [preprocess_landings()].
#'
#' @param submissions Output of [validation_submissions()].
#' @param mesh_limit Largest plausible mesh size, in millimetres.
#' @return A tibble: `submission_id`, `mesh_size`, `alert_number`.
#' @keywords validation
#' @export
validate_mesh <- function(submissions, mesh_limit) {
  submissions %>%
    dplyr::transmute(
      .data$submission_id,
      .data$mesh_size,
      alert_number = dplyr::if_else(
        .data$mesh_size < 0 | .data$mesh_size > mesh_limit, 20, NA_real_
      ),
      mesh_size = dplyr::if_else(
        is.na(.data$alert_number), .data$mesh_size, NA_real_
      )
    )
}

#' Validate the recorded number of gleaners
#'
#' @param submissions Output of [validation_submissions()].
#' @inheritParams univOutl::LocScaleB
#' @param k_gleaners Extension of bounds for the number of gleaners.
#' @return A tibble: `submission_id`, `n_gleaners`, `alert_number`.
#' @keywords validation
#' @export
validate_gleaners <- function(submissions, method, k_gleaners) {
  submissions %>%
    dplyr::transmute(
      .data$submission_id,
      .data$n_gleaners,
      alert_number = alert_outlier(
        x = .data$n_gleaners,
        alert_if_larger = 21, logt = TRUE, k = k_gleaners
      ),
      n_gleaners = dplyr::if_else(
        is.na(.data$alert_number), .data$n_gleaners, NA_real_
      )
    )
}

#' Validate the recorded fuel use
#'
#' Alert 23 never reaches a flag string: fuel is one of the three frames that
#' feed the validated artefact but not the flag composition in
#' [validate_landings()].
#'
#' @param submissions Output of [validation_submissions()].
#' @inheritParams univOutl::LocScaleB
#' @param k_fuel Extension of bounds for the fuel volume.
#' @return A tibble: `submission_id`, `fuel`, `alert_number`.
#' @keywords validation
#' @export
validate_fuel <- function(submissions, method, k_fuel) {
  submissions %>%
    dplyr::transmute(
      .data$submission_id,
      .data$fuel,
      alert_number.1 = ifelse(.data$fuel < 0, 23, NA_real_),
      alert_number.2 = alert_outlier(
        x = .data$fuel,
        alert_if_larger = 23, logt = TRUE, k = k_fuel
      ),
      alert_number.3 = dplyr::case_when(
        .data$fuel > 0 & isFALSE(.data$has_boat) ~ 23, TRUE ~ NA_real_
      ),
      alert_number = dplyr::coalesce(
        .data$alert_number.1, .data$alert_number.2, .data$alert_number.3
      ),
      fuel = dplyr::if_else(is.na(.data$alert_number), .data$fuel, NA_real_)
    ) %>%
    dplyr::select("submission_id", "fuel", "alert_number")
}

#' Resolve the recorded catch-preservation method
#'
#' Maps the recorded code to its label. Raises no alert.
#'
#' @param submissions Output of [validation_submissions()].
#' @return A tibble: `submission_id`, `conservation_place`, `alert_number`.
#' @keywords validation
#' @export
validate_conservation <- function(submissions) {
  submissions %>%
    dplyr::select("submission_id", conservation_code = "conservation_code") %>%
    dplyr::left_join(conservation_labels(), by = "conservation_code") %>%
    dplyr::select(-"conservation_code") %>%
    dplyr::mutate(alert_number = NA_real_)
}

# The catch-preservation code -> label lookup recorded on the survey form.
conservation_labels <- function() {
  tibble::tribble(
    ~conservation_code, ~conservation_place,
    "1", "Open",
    "2", "Shade",
    "3", "Box",
    "4", "Ice box",
    "5", "Other"
  )
}

#' Carry through the recorded happiness rating
#'
#' Raises no alert; it is here so the rating reaches the validated artefact
#' alongside everything else.
#'
#' @param submissions Output of [validation_submissions()].
#' @return A tibble: `submission_id`, `happiness`, `alert_number`.
#' @keywords validation
#' @export
validate_happiness <- function(submissions) {
  submissions %>%
    dplyr::transmute(
      .data$submission_id,
      .data$happiness,
      alert_number = NA_real_
    )
}
