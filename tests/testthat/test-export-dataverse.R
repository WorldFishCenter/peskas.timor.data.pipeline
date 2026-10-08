# Two columns stand in for the API table; `fields` is the shape of the API's
# `/metadata/landings` answer.
trips <- tibble::tibble(
  trip_id = c("TRIP_1", "TRIP_1", "TRIP_2"),
  landing_date = as.Date(c("2026-01-02", "2026-01-02", NA)),
  catch_kg = c(1.5, NA, 3)
)
fields <- list(
  catch_kg = list(
    name = "catch_kg",
    description = "Weight",
    data_type = "float",
    unit = "kg",
    ontology_url = NULL,
    url = "https://example.org/kg"
  ),
  landing_date = list(
    name = "landing_date",
    description = "Landed | brought to shore",
    data_type = "date"
  ),
  trip_id = list(name = "trip_id", description = "Trip", data_type = "string")
)

test_that("the column table follows the data, not the API's order", {
  table <- strsplit(codebook_table(names(trips), fields), "\n")[[1]]
  expect_length(table, 2 + ncol(trips))
  expect_match(table[3], "^\\| `trip_id` \\| Trip \\| string \\|  \\|  \\|$")
  expect_match(table[4], "Landed \\| brought to shore", fixed = TRUE)
  expect_match(table[5], "| kg | [link](https://example.org/kg) |", fixed = TRUE)
})

test_that("a column the API does not describe stops the release", {
  expect_error(codebook_table(c("trip_id", "new_column"), fields), "new_column")
})

# Runs `upload_dataverse()` against a recorded Dataverse: returns the calls it
# made, as "VERB path".
release <- function(dataset_doi, config, current = list()) {
  calls <- character()
  conf <- structure(
    list(
      country = "timor",
      api = list(trips = list(validated = list(cloud_path = "", file_prefix = ""))),
      storage = list(google = list()),
      export_dataverse = list(
        server = "dataverse.test",
        dataverse_id = "peskas",
        dataset_doi = dataset_doi
      )
    ),
    config = config
  )
  local_mocked_bindings(
    read_config = function() conf,
    api_fields = function(conf) fields,
    dataverse_api = function(verb, path, ...) {
      calls <<- c(calls, paste(verb, path))
      if (endsWith(path, "/files")) {
        return(current)
      }
      if (endsWith(path, "/datasets")) {
        return(list(persistentId = "doi:10.1/NEW"))
      }
      list()
    }
  )
  local_mocked_bindings(
    download_parquet_from_cloud = function(...) trips,
    .package = "coasts"
  )
  upload_dataverse(log_threshold = logger::FATAL)
  calls
}

test_that("with no DOI the dataset is created as a draft and not published", {
  calls <- release(dataset_doi = NULL, config = "production")
  expect_equal(calls[1], "POST dataverses/peskas/datasets")
  expect_equal(sum(calls == "POST datasets/:persistentId/add"), 2)
  expect_false(any(grepl(":publish", calls)))
})

test_that("a release replaces what changed and publishes in production only", {
  csv <- tempfile(fileext = ".csv")
  readr::write_csv(trips, csv, na = "")
  # Dataverse lists the ingested table as `.tab`, with the checksum of the csv.
  current <- list(
    list(
      label = "timor_landings.tab",
      dataFile = list(id = 7, md5 = unname(tools::md5sum(csv)))
    ),
    list(label = "README.md", dataFile = list(id = 8, md5 = "stale"))
  )

  calls <- release("doi:10.1/OLD", "production", current)
  expect_equal(calls[1], "PUT datasets/:persistentId/versions/:draft")
  expect_true("POST files/8/replace" %in% calls)
  expect_false("POST files/7/replace" %in% calls)
  expect_false(any(grepl("/add$", calls)))
  expect_equal(tail(calls, 1), "POST datasets/:persistentId/actions/:publish")

  calls <- release("doi:10.1/OLD", "default", current)
  expect_false(any(grepl(":publish", calls)))
})
