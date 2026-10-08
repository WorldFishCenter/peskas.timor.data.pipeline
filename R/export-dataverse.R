#' Publish the validated landings on Harvard Dataverse
#'
#' Releases the table the Peskas Fishery Data API serves for Timor-Leste (the
#' `trips-validated` parquet written by [export_api_validated()]) as a new
#' version of one Dataverse dataset.
#'
#' @details
#' Two files are released, both built here:
#'
#' * `timor_landings.csv`, the API table as it is, with empty cells for missing
#'   values.
#' * `README.md`, filled in from `inst/export/README.md`. Its column table is
#'   read from the API's `/metadata/landings` endpoint at run time, so no column
#'   is described in this package, and a column the API does not describe stops
#'   the release.
#'
#' The dataset record (title, authors, licence, links) is
#' `inst/export/dataset-fields.json`. It is sent on every run, so edit it there:
#' a change made on the Dataverse website is overwritten by the next release.
#'
#' `export_dataverse$dataset_doi` in `inst/config.yml` names the dataset:
#'
#' * **Empty**: the dataset is created as an unpublished draft and its DOI is
#'   logged. Review the draft, put the DOI in the config, and run again.
#' * **Set**: the files whose content changed are replaced and a new major
#'   version is published. Outside the `production` configuration the draft is
#'   left unpublished.
#'
#' The calls follow the Dataverse native API,
#' <https://guides.dataverse.org/en/latest/api/native-api.html>.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#'   threshold for the logging infrastructure. See [logger::log_levels] for more
#'   details
#' @return Invisibly, the DOI of the dataset.
#'
#' @keywords workflow export
#' @export
#'
upload_dataverse <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()
  dv <- conf$export_dataverse

  logger::log_info("Downloading the validated API table...")
  trips <- coasts::download_parquet_from_cloud(
    prefix = file.path(
      conf$api$trips$validated$cloud_path,
      conf$api$trips$validated$file_prefix
    ),
    provider = conf$storage$google$key,
    options = conf$storage$google$options_api
  )

  period <- range(trips$landing_date, na.rm = TRUE)
  values <- list(
    start = period[1],
    end = period[2],
    released = Sys.Date(),
    rows = format(nrow(trips), big.mark = ","),
    trips = format(dplyr::n_distinct(trips$trip_id), big.mark = ","),
    version = utils::packageVersion("peskas.timor.data.pipeline"),
    codebook = codebook_table(names(trips), api_fields(conf))
  )

  landings <- file.path(tempdir(), paste0(conf$country, "_landings.csv"))
  readme <- file.path(tempdir(), "README.md")
  readr::write_csv(trips, landings, na = "")
  writeLines(fill_template("README.md", values), readme)
  metadata <- fill_template("dataset-fields.json", values)

  doi <- dv$dataset_doi
  if (is.null(doi)) {
    logger::log_info("Creating the dataset in '{dv$dataverse_id}'...")
    doi <- dataverse_api(
      "POST",
      paste0("dataverses/", dv$dataverse_id, "/datasets"),
      conf,
      body = paste0('{"datasetVersion":', metadata, "}"),
      httr::content_type_json()
    )$persistentId
  } else {
    logger::log_info("Updating the record of {doi}...")
    dataverse_api(
      "PUT",
      "datasets/:persistentId/versions/:draft",
      conf,
      query = list(persistentId = doi),
      body = metadata,
      httr::content_type_json()
    )
  }

  current <- dataverse_api(
    "GET",
    "datasets/:persistentId/versions/:latest/files",
    conf,
    query = list(persistentId = doi)
  )
  dataverse_upload(
    landings,
    "Validated landings: one row per catch per fishing trip. See README.md.",
    current,
    doi,
    conf
  )
  dataverse_upload(
    readme,
    "What the data is, what each column means, and how it is produced.",
    current,
    doi,
    conf
  )

  draft <- paste0("https://", dv$server, "/dataset.xhtml?persistentId=", doi)
  if (is.null(dv$dataset_doi)) {
    logger::log_info(
      "Created {doi} as a draft: {draft}. To publish it, set ",
      "export_dataverse.dataset_doi to it in inst/config.yml and run again."
    )
    return(invisible(doi))
  }
  if (attr(conf, "config") != "production") {
    logger::log_info("Not production, draft left unpublished: {draft}")
    return(invisible(doi))
  }

  dataverse_wait(doi, conf)
  logger::log_info("Publishing a new version of {doi}...")
  dataverse_api(
    "POST",
    "datasets/:persistentId/actions/:publish",
    conf,
    # Dataverse answers 409 until the edits above are indexed, hence the
    # retries.
    query = list(persistentId = doi, type = "major", assureIsIndexed = "true"),
    times = 20
  )
  invisible(doi)
}

# The fields of the landings table as the Peskas API describes them
# (`/metadata/landings`, which needs no key): a list keyed by column name.
api_fields <- function(conf) {
  res <- httr::GET(paste0(conf$api$url, "/metadata/landings"))
  httr::stop_for_status(res, task = "read the field metadata of the Peskas API")
  httr::content(res)$fields
}

# The README's column table, in the order of the data. A column the API does
# not describe stops the release, so the README cannot fall behind the file.
codebook_table <- function(columns, fields) {
  undescribed <- setdiff(columns, names(fields))
  if (length(undescribed) > 0) {
    stop(
      "The Peskas API describes no field for: ",
      paste(undescribed, collapse = ", "),
      call. = FALSE
    )
  }

  rows <- purrr::map_chr(fields[columns], function(field) {
    link <- field$ontology_url %||% field$url
    cells <- c(
      paste0("`", field$name, "`"),
      field$description,
      field$data_type,
      field$unit %||% "",
      if (is.null(link)) "" else paste0("[link](", link, ")")
    )
    cells <- gsub("|", "\\|", cells, fixed = TRUE)
    paste0("| ", paste(cells, collapse = " | "), " |")
  })

  paste(
    c(
      "| Column | Description | Type | Unit | Reference |",
      "|---|---|---|---|---|",
      rows
    ),
    collapse = "\n"
  )
}

# Fill the `<<name>>` placeholders of a template in `inst/export/`.
fill_template <- function(name, values) {
  template <- readr::read_file(system.file(
    "export",
    name,
    package = "peskas.timor.data.pipeline",
    mustWork = TRUE
  ))
  as.character(glue::glue_data(
    values,
    template,
    .open = "<<",
    .close = ">>",
    .trim = FALSE
  ))
}

# One call to the Dataverse native API; `path` is what follows `/api/`. Fails
# with the server's own message. `times` above 1 retries a refused call.
dataverse_api <- function(verb, path, conf, ..., query = NULL, times = 1) {
  dv <- conf$export_dataverse
  res <- httr::RETRY(
    verb,
    paste0("https://", dv$server, "/api/", path),
    httr::add_headers(`X-Dataverse-key` = dv$token),
    query = query,
    ...,
    times = times,
    pause_min = 30,
    pause_cap = 30,
    terminate_on = c(400, 401, 403, 404)
  )
  httr::stop_for_status(
    res,
    task = paste("call Dataverse:", httr::content(res)$message)
  )
  httr::content(res)$data
}

# Add `file` to the dataset, or replace the file of the same name. Dataverse
# renames a table it has ingested to `.tab` and refuses a replacement with
# identical content, hence the extension-free match and the checksum, which it
# keeps for the file as uploaded.
dataverse_upload <- function(file, description, current, doi, conf) {
  name <- tools::file_path_sans_ext(basename(file))
  old <- purrr::detect(
    current,
    ~ tools::file_path_sans_ext(.x$label) == name
  )
  if (identical(old$dataFile$md5, unname(tools::md5sum(file)))) {
    logger::log_info("{basename(file)} has not changed")
    return(invisible(NULL))
  }

  dataverse_wait(doi, conf)
  logger::log_info("Uploading {basename(file)}...")
  body <- list(
    file = httr::upload_file(file),
    jsonData = as.character(jsonlite::toJSON(
      list(description = description, forceReplace = TRUE),
      auto_unbox = TRUE
    ))
  )
  if (is.null(old)) {
    dataverse_api(
      "POST",
      "datasets/:persistentId/add",
      conf,
      query = list(persistentId = doi),
      body = body
    )
  } else {
    dataverse_api(
      "POST",
      paste0("files/", old$dataFile$id, "/replace"),
      conf,
      body = body
    )
  }
  invisible(NULL)
}

# Wait for Dataverse to finish ingesting: a locked dataset refuses uploads and
# publication.
dataverse_wait <- function(doi, conf) {
  for (attempt in seq_len(120)) {
    locks <- dataverse_api(
      "GET",
      "datasets/:persistentId/locks",
      conf,
      query = list(persistentId = doi)
    )
    if (length(locks) == 0) {
      return(invisible(NULL))
    }
    logger::log_info("Dataverse is still processing {doi}, waiting...")
    Sys.sleep(30)
  }
  stop("Dataverse still locks ", doi, " after an hour.", call. = FALSE)
}
