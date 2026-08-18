#' Validate landings
#'
#' Downloads the weighted long catch table from cloud storage, runs every
#' validator in `R/validation-functions.R`, and publishes two things: the validated
#' landings artefacts, and the per-submission flags the enumerators act on.
#'
#' By default outlier identification uses the median absolute deviation (MAD).
#'
#' @section Outputs:
#' * `<surveys.landings.validated.file_prefix>__*.rds` — the **nested**
#'   artefact, one row per submission with the `landing_catch` list-column. This
#'   is the portal's input and its column names are load-bearing:
#'   `format_public_data()` reads `municipality`, `landing_site`,
#'   `propulsion_gear`, `trip_length`, `catch_preservation` and the
#'   `fisher_number_*` trio by name. Unchanged by migration Phase 5.
#' * `<...>_long__*.parquet` — the same content in the **flat long** shape, one
#'   row per (submission, catch, length bin), under the standard column names.
#'   Written for migration Phase 6's API export; nothing reads it yet.
#' * `<surveys.landings.validation.flags.file_prefix>__*.parquet` — a versioned
#'   snapshot of the flags. [coasts::mdb_collection_push()] replaces a
#'   collection wholesale, so this is the only history of what was flagged when.
#'
#' @section Flags sink:
#' Flags go to the **shared** cross-country validation database
#' (`storage.mongodb.databases.validation`), one
#' `surveys_flags-<asset_id>` collection per live form plus the matching
#' `enumerators_stats-<asset_id>`. v1 is frozen and gets neither. This replaced
#' the Google Sheets `flags` tab in migration Phase 5.
#'
#' Where a token is configured, the current KoBoToolbox validation status of the
#' already-flagged submissions is read first, so an approval an enumerator
#' entered by hand is preserved rather than overwritten. Writing a status *back*
#' to KoBoToolbox is [sync_validation_status()], which the recurring pipeline
#' deliberately does not call.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a threshold for the logging infrastructure. See [logger::log_levels] for more details
#' @keywords workflow validation
#' @return no outputs. This function is used for it's side effects
#'
#' @importFrom rlang .data
#' @export
#'
validate_landings <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  conf <- read_config()
  metadata <- get_preprocessed_sheets(conf)
  landings <- get_weighted_landings(conf)
  submissions <- validation_submissions(landings)

  # read arguments for outliers identification
  default_max_limit <- conf$validation$landings$default$max
  default_method <- conf$validation$landings$default$method
  default_k <- conf$validation$landings$default$k
  cook_dist <- conf$validation$landings$cook_dist

  # Every deployed IMEI, not only those with an install record: the
  # `device_installs` narrowing has been switched off for years.
  deployed_imeis <- na.omit(metadata$devices$device_imei)

  logger::log_info("Validating IMEIs...")
  imei_alerts <- validate_imeis(submissions, deployed_imeis)

  logger::log_info("Validating surveys trips...")
  surveys_time_alerts <- validate_surveys_time(
    submissions = submissions,
    hrs = conf$validation$landings$survey_time$max_duration %||% default_max_limit,
    submission_delay = conf$validation$landings$survey_time$submission_delay
  )

  logger::log_info("Validating catches values...")
  regular_landings <- validate_landing_regularity(landings)
  surveys_price_alerts <- validate_catch_price(
    regular_landings = regular_landings$regular_landings,
    method = conf$validation$landings$prices$method %||% default_method,
    k = conf$validation$landings$prices$k %||% default_k
  )

  logger::log_info("Validating catches parameters...")
  catch_params <- validate_catch_params(
    regular_landings$regular_landings,
    k_ind = conf$validation$landings$catch$n_individuals$k
  )
  price_weight <- validate_price_weight(
    catch_params = catch_params,
    price_alerts = surveys_price_alerts,
    non_regular_ids = regular_landings$regularity_alerts,
    cook_dist = cook_dist,
    price_weight_min = conf$validation$landings$price_per_weight$min_limit,
    price_weight_max = conf$validation$landings$price_per_weight$max_limit
  )

  logger::log_info("Validating survey attributes...")
  vessel_type_alerts <- validate_vessel_type(submissions)
  gear_type_alerts <- validate_gear_type(submissions)
  site_alerts <- validate_sites(
    submissions,
    metadata$stations, metadata$reporting_unit
  )
  n_fishers_alerts <- validate_n_fishers(
    submissions,
    method = conf$validation$landings$n_fishers$method %||% default_method,
    k = conf$validation$landings$n_fishers$k %||% default_k
  )
  habitat_alerts <- validate_habitat(submissions)
  mesh_alerts <- validate_mesh(submissions,
    mesh_limit = conf$validation$landings$mesh
  )
  gleaners_alerts <- validate_gleaners(
    submissions,
    method = default_method,
    k_gleaners = conf$validation$landings$gleaners$k
  )
  fuel_alerts <- validate_fuel(
    submissions,
    method = default_method,
    k_fuel = conf$validation$landings$fuel$k
  )
  conservation_alerts <- validate_conservation(
    submissions,
    metadata_conservation = metadata$conservation
  )
  happiness_alerts <- validate_happiness(submissions)

  # CREATE VALIDATED OUTPUT -----------------------------------------------

  logger::log_info("Assembling the validated landings")

  validated_catch <- rename_validated_catch(price_weight$catch)

  validated_landings <-
    list(
      imei_alerts,
      surveys_time_alerts$validated_dates,
      surveys_time_alerts$validated_duration,
      price_weight$alerts,
      vessel_type_alerts,
      gear_type_alerts,
      site_alerts,
      n_fishers_alerts,
      habitat_alerts,
      mesh_alerts,
      gleaners_alerts,
      fuel_alerts,
      conservation_alerts,
      happiness_alerts
    ) %>%
    purrr::map(~ dplyr::select(.x, -"alert_number")) %>%
    purrr::reduce(dplyr::left_join, by = "submission_id") %>%
    dplyr::select(
      landing_id = "submission_id",
      landing_date = "date",
      tracker_imei = "imei",
      "trip_length",
      "catch_price",
      landing_site = "station_name",
      municipality = "reporting_region",
      habitat = "habitat_type",
      tidyselect::starts_with("fisher_number"),
      gear = "gear_type",
      "mesh_size",
      propulsion_gear = "vessel_type",
      "n_gleaners",
      "fuel",
      catch_preservation = "conservation_place",
      "happiness"
    )

  # The one validated artefact since migration Phase 8. Until then a second,
  # nested `.rds` was written beside it for the portal path; that path now gets
  # the same shape from `get_validated_landings()`, which re-nests this table on
  # read. The two were proven interchangeable first — see that function.
  logger::log_info("Uploading the long validated catch table")
  coasts::upload_parquet_to_cloud(
    data = long_validated_landings(
      validated_catch, validated_landings, api_submission_extras(landings)
    ),
    prefix = conf$surveys$landings$validated_long$file_prefix,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country")
  )

  # HANDLE FLAGS ------------------------------------------------------------

  # Column order here is the alert contract: `unite()` composes the flag string
  # in it, so "10-17" means a date alert and a price-per-weight alert, in that
  # order. Fuel, conservation and happiness are deliberately absent — they
  # reach the validated artefact but never a flag.
  alerts <-
    list(
      imei = imei_alerts,
      date = surveys_time_alerts$validated_dates,
      duration = surveys_time_alerts$validated_duration,
      price_weight = price_weight$alerts,
      vessel = vessel_type_alerts,
      gear = gear_type_alerts,
      site = site_alerts,
      n_fishers = n_fishers_alerts,
      habitat = habitat_alerts,
      mesh = mesh_alerts,
      gleaners = gleaners_alerts
    ) %>%
    purrr::imap(function(x, name) {
      dplyr::select(
        x, "submission_id",
        !!paste0("alert_", name) := "alert_number"
      )
    }) %>%
    purrr::reduce(dplyr::full_join, by = "submission_id") %>%
    tidyr::unite(
      col = "alert",
      tidyselect::starts_with("alert_"),
      sep = "-",
      na.rm = TRUE
    )

  flags <-
    submissions %>%
    dplyr::select(
      "submission_id", "survey_version", "submitted_by", "submission_date"
    ) %>%
    dplyr::mutate(submission_date = lubridate::as_date(.data$submission_date)) %>%
    dplyr::left_join(alerts, by = "submission_id") %>%
    dplyr::arrange(.data$submission_date, .data$submission_id) %>%
    dplyr::mutate(
      alert = dplyr::if_else(
        is.na(.data$alert) | .data$alert == "", "0", .data$alert
      ),
      flag_date = lubridate::today("GMT"),
      validated = FALSE,
      comments = NA_character_,
      validated_when_ymd = as.Date(NA)
    )

  logger::log_info(
    "{sum(flags$alert != '0')} of {nrow(flags)} submissions flagged"
  )

  flags_filename <- conf$surveys$landings$validation$flags$file_prefix
  coasts::upload_parquet_to_cloud(
    data = dplyr::select(
      flags,
      "submission_id", "submission_date", "flag_date", "alert", "validated",
      "validated_when_ymd", "comments"
    ),
    prefix = flags_filename,
    provider = conf$storage$google$key,
    options = coasts::resolve_storage_opts(conf, "country")
  )

  push_validation_flags(conf, flags)
}

#' Push the validation flags to the shared validation database
#'
#' One `surveys_flags-<asset_id>` collection per **live** form, plus the
#' `enumerators_stats-<asset_id>` long form (one row per raised alert) the
#' cross-country validation UI reads. v1 is frozen: it has no live asset and
#' gets no collection.
#'
#' @param conf The configuration file.
#' @param flags The flags frame assembled by [validate_landings()].
#' @return Invisibly, the flags frame that was pushed.
#' @keywords validation
#' @export
push_validation_flags <- function(conf, flags) {
  mdb <- conf$storage$mongodb
  if (is.null(mdb$connection_strings$validation) ||
    !nzchar(mdb$connection_strings$validation)) {
    logger::log_warn(
      "MONGODB_CONNECTION_STRING_VALIDATION is not set - flags not pushed. ",
      "The versioned snapshot in cloud storage is the only record of this run."
    )
    return(invisible(flags))
  }

  # v1 is frozen; only the live forms have a collection.
  purrr::walk(c("v2", "v3"), function(version) {
    asset_id <- conf$ingestion$landings[[version]]$asset_id
    form_flags <- dplyr::filter(flags, .data$survey_version == version)
    if (nrow(form_flags) == 0) {
      logger::log_warn("No {version} submissions to flag")
      return(invisible(NULL))
    }

    collection <- paste(mdb$databases$validation$collections$flags,
      asset_id,
      sep = "-"
    )

    out <-
      form_flags %>%
      dplyr::transmute(
        .data$submission_id,
        .data$survey_version,
        .data$submitted_by,
        .data$submission_date,
        # The shared schema treats an absent flag as "nothing to review".
        alert_flag = dplyr::if_else(
          .data$alert == "0", NA_character_,
          gsub("-", ", ", .data$alert)
        )
      ) %>%
      merge_kobo_validation_status(conf, version)

    logger::log_info("Pushing {nrow(out)} {version} flags to {collection}")
    coasts::mdb_collection_push(
      data = out,
      connection_string = mdb$connection_strings$validation,
      db_name = mdb$databases$validation$database_name,
      collection_name = collection
    )

    coasts::mdb_collection_push(
      data = out %>%
        dplyr::filter(!is.na(.data$alert_flag)) %>%
        tidyr::separate_rows("alert_flag", sep = ",\\s*") %>%
        dplyr::select(-tidyselect::starts_with("valid")),
      connection_string = mdb$connection_strings$validation,
      db_name = mdb$databases$validation$database_name,
      collection_name = paste(
        mdb$databases$validation$collections$enumerators_stats,
        asset_id,
        sep = "-"
      )
    )
  })

  invisible(flags)
}

# Derive the validation status each submission should carry, preserving an
# approval a human entered in KoBoToolbox by hand. Reading the status is a GET
# and safe from any environment; writing it back is `sync_validation_status()`.
merge_kobo_validation_status <- function(flags, conf, version) {
  statuses <- kobo_validation_status(conf, version)

  flags %>%
    dplyr::left_join(statuses, by = "submission_id") %>%
    dplyr::mutate(
      pipeline_user = conf$ingestion$landings[[version]]$username,
      validation_status = dplyr::case_when(
        # Somebody other than the pipeline account ruled on this submission
        !is.na(.data$validated_by) &
          .data$validated_by != .data$pipeline_user ~ .data$validation_status,
        !is.na(.data$alert_flag) ~ "validation_status_not_approved",
        TRUE ~ "validation_status_approved"
      ),
      validated_by = dplyr::coalesce(.data$validated_by, .data$pipeline_user),
      # No KoBoToolbox status for this submission, whether because the read
      # failed or because the submission has since been deleted there.
      fetch_error = is.na(.data$fetch_error)
    ) %>%
    dplyr::select(-"pipeline_user")
}

# Every submission's current status in KoBoToolbox, so an approval a human
# entered there is not overwritten. One paginated request per 1,000 submissions;
# see `coasts::list_validation_statuses()` for why this is not done one at a time.
kobo_validation_status <- function(conf, version) {
  empty <- tibble::tibble(
    submission_id = integer(),
    validation_status = character(),
    validated_at = lubridate::as_datetime(character()),
    validated_by = character(),
    fetch_error = logical()
  )

  ingestion <- conf$ingestion$landings[[version]]
  if (is.null(ingestion$username) || !nzchar(ingestion$username)) {
    logger::log_warn(
      "No KoBoToolbox credentials - manual approvals for {version} cannot be ",
      "read and will be overwritten by this run's flags."
    )
    return(empty)
  }

  logger::log_info("Reading KoBoToolbox validation statuses for {version}")
  tryCatch(
    coasts::list_validation_statuses(
      asset_id = ingestion$asset_id,
      username = ingestion$username,
      password = ingestion$password
    ),
    error = function(e) {
      logger::log_warn(
        "Could not read {version} validation statuses: {conditionMessage(e)}"
      )
      empty
    }
  )
}

#' Write validation statuses back to KoBoToolbox
#'
#' Sets each submission's KoBoToolbox validation status from the flags already
#' pushed to the shared validation database, so the enumerators see the review
#' queue in the form itself. Only submissions whose status differs are patched.
#'
#' **Not part of the recurring pipeline, on purpose.** There is no development
#' KoBoToolbox instance, so this mutates the two live forms whatever
#' `R_CONFIG_ACTIVE` says — running it from a migration branch would change
#' production review state. Wire it into a workflow once that is a deliberate
#' decision rather than a side effect.
#'
#' @param versions Form versions to synchronise. v1 is frozen and has no
#'   collection.
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure.
#' @return Invisibly, a tibble of per-submission update results.
#' @keywords workflow validation
#' @export
sync_validation_status <- function(versions = c("v2", "v3"),
                                   log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()
  mdb <- conf$storage$mongodb

  results <- purrr::map_dfr(versions, function(version) {
    ingestion <- conf$ingestion$landings[[version]]
    if (is.null(ingestion$username) || !nzchar(ingestion$username)) {
      stop("KoBoToolbox credentials are required to write statuses back")
    }

    flags <- coasts::mdb_collection_pull(
      connection_string = mdb$connection_strings$validation,
      db_name = mdb$databases$validation$database_name,
      collection_name = paste(mdb$databases$validation$collections$flags,
        ingestion$asset_id,
        sep = "-"
      )
    )

    # Patching a submission to the status it already has is a wasted request,
    # and there is one request per submission.
    pending <- flags %>%
      dplyr::select("submission_id", "validation_status") %>%
      dplyr::anti_join(
        coasts::list_validation_statuses(
          asset_id = ingestion$asset_id,
          username = ingestion$username,
          password = ingestion$password
        ) %>%
          dplyr::select("submission_id", "validation_status"),
        by = c("submission_id", "validation_status")
      )

    logger::log_info(
      "Patching {nrow(pending)} of {nrow(flags)} {version} statuses in KoBoToolbox"
    )
    purrr::pmap_dfr(
      list(pending$submission_id, pending$validation_status),
      function(submission_id, status) {
        coasts::update_validation_status(
          submission_id = submission_id,
          asset_id = ingestion$asset_id,
          username = ingestion$username,
          password = ingestion$password,
          status = status
        )
      }
    )
  })

  failures <- sum(!results$update_success)
  if (failures > 0) logger::log_warn("{failures} status updates failed")
  invisible(results)
}

# The validated catch columns, under the names the portal has always used.
# `catch_outcome` and `scientific_name` ride along for the long table and the
# API export; `nest_landing_catch()` — now in get-cloud-files.R, since Phase 8
# reshapes on read rather than on write — drops them again.
rename_validated_catch <- function(catch) {
  catch %>%
    dplyr::transmute(
      submission_id = as.integer(.data$submission_id),
      .data$n_catch,
      .data$catch_taxon,
      .data$catch_use,
      .data$catch_outcome,
      .data$scientific_name,
      .data$length_type,
      .data$length,
      number_of_fish = .data$n_individuals,
      catch = .data$weight,
      dplyr::across(dplyr::all_of(setdiff(catch_value_cols(), "weight")))
    )
}

# The flat long validated table, and since migration Phase 8 the only one: the
# validated submission columns joined back onto the validated catch rows, under
# standard names. `catch_kg` rather than grams, because that is what the
# cross-country API schema migration Phase 6 conforms to publishes.
#
# `submission_extras` carries the columns the retired *nested* artefact never
# had — the form version and the GAUL administrative codes — so that
# `export_api_validated()` is a projection of this table rather than a second
# reconstruction of it.
long_validated_landings <- function(validated_catch,
                                    validated_landings,
                                    submission_extras) {
  validated_landings %>%
    dplyr::rename(
      submission_id = "landing_id",
      trip_duration = "trip_length",
      vessel_type = "propulsion_gear",
      catch_habitat = "habitat"
    ) %>%
    dplyr::mutate(
      n_fishers = sum_fishers(
        .data$fisher_number_man,
        .data$fisher_number_woman,
        .data$fisher_number_child
      )
    ) %>%
    dplyr::left_join(submission_extras, by = "submission_id") %>%
    dplyr::left_join(
      validated_catch %>%
        dplyr::transmute(
          .data$submission_id,
          .data$n_catch,
          .data$catch_taxon,
          .data$scientific_name,
          .data$catch_use,
          .data$catch_outcome,
          .data$length_type,
          .data$length,
          n_individuals = .data$number_of_fish,
          catch_kg = .data$catch / 1000,
          dplyr::across(tidyselect::ends_with("_mu"))
        ),
      by = "submission_id"
    )
}

# The submission-level columns the validators never touch and the nested
# artefact therefore never carried.
api_submission_extras <- function(landings) {
  landings %>%
    dplyr::distinct(
      .data$submission_id, .data$survey_version,
      .data$gaul_1_code, .data$gaul_1_name,
      .data$gaul_2_code, .data$gaul_2_name
    ) %>%
    dplyr::mutate(submission_id = as.integer(.data$submission_id))
}

# NOTE: three dead helpers used to live here, all removed in migration Phase 1
# (AUDIT.md §8.1 and §8.2):
#
#   get_preprocessed_metadata()  a second, broken definition reading the
#                                long-removed `conf$metadata$airtable$name`.
#                                Collation put this file after
#                                get-cloud-files.R, so it *shadowed* the
#                                correct exported definition at
#                                get-cloud-files.R:110. That one now wins.
#   get_validation_tables()      read `conf$validation$airtable$*`, removed
#                                with the orphaned air_* Airtable client.
#   get_preprocessed_landings()  read `conf$surveys$landings$file_prefix`,
#                                a key that has never existed.
#
# None were exported and none were called.

# NOTE: `get_merged_landings()` moved to get-cloud-files.R with the other
# storage accessors in migration Phase 4, and split in two: the merged table is
# parquet now, and so is the weight artefact since Phase 5.
