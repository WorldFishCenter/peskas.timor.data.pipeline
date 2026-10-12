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

test_that("validate_price_weight() reports 11 on its own and does not judge its price per kg", {
  # 1: individuals outlier (11), price per kg out of band on what weight is left.
  # 2: individuals outlier (11) and revenue too large (6).
  # 3: price per kg out of band only (17).
  catch <- tibble::tibble(
    submission_id = c(1, 1, 2, 3),
    weight = c(NA, 100, 1000, 1000),
    n_individuals = c(NA, 5, 5, 5),
    alert_number = c(11, NA, NA, NA)
  )
  for (col in setdiff(catch_value_cols(), "weight")) catch[[col]] <- 1
  out <- validate_price_weight(
    catch_params = list(
      alerts = tibble::tibble(submission_id = c(1, 2), alert_number = 11),
      catch = catch
    ),
    price_alerts = tibble::tibble(
      submission_id = c(1, 2, 3),
      catch_price = c(50, 2000, 100),
      alert_number = c(NA, 6, NA)
    ),
    non_regular_ids = tibble::tibble(submission_id = numeric(), alert_regularity = numeric()),
    price_weight_min = 0.25,
    price_weight_max = 30
  )

  alerts <- dplyr::arrange(out$alerts, .data$submission_id)
  expect_equal(alerts$alert_number, c(NA, 6, 17))
  expect_equal(sort(out$individuals$submission_id), c(1, 2))
  # Blanking is unchanged: both 11 landings lose their price and catch values.
  expect_true(all(is.na(alerts$catch_price[1:2])))
  expect_true(all(is.na(out$catch$weight[out$catch$submission_id %in% c(1, 2)])))
})
