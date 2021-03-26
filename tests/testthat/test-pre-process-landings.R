test_that("attachments are being nested properly", {

  dummy_landings <- tidyr::tibble(
    `_id` = "123",
    `_attachments.0.download_url` = "http://url-1.com",
    `_attachments.0.id` = "01",
    `_attachments.1.download_url` = "http://url-2.com",
    `_attachments.1.id` = "02",
    `_attachments.2.download_url` = NA,
    `_attachments.2.id` = NA)

  nested <- pt_nest_attachments(dummy_landings)

  expect_type(nested$`_attachments`, "list")
  expect_s3_class(nested$`_attachments`[[1]], "data.frame")
  expect_equal(nrow(nested$`_attachments`[[1]]), 2)

})
