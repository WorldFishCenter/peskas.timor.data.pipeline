test_that("validated_ids() keeps unflagged and approved, drops flagged and rejected", {
  flags <- tibble::tibble(
    submission_id = 1:5,
    alert = c("0", "10", "10-17", "0", "4")
  )
  reviews <- tibble::tibble(
    submission_id = c(2L, 4L, 5L),
    validation_status = c(
      "validation_status_approved",
      "validation_status_not_approved",
      "validation_status_on_hold"
    )
  )

  expect_equal(validated_ids(flags, reviews), c(1L, 2L))
  expect_equal(validated_ids(flags, reviews[0, ]), c(1L, 4L))
})

test_that("validate_surveys_time() flags a future landing, not a late submission", {
  submissions <- tibble::tibble(
    submission_id = 1:2,
    landing_date = as.Date(c("2026-01-01", "2026-03-01")),
    submission_date = as.POSIXct(c("2026-06-01", "2026-02-01"), tz = "Asia/Dili"),
    trip_duration = 5
  )

  dates <- validate_surveys_time(submissions, hrs = 96)$validated_dates
  expect_equal(dates$alert_number, c(NA, 4))
})
