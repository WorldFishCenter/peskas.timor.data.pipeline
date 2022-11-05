test_that("attachments are being nested properly", {
  dummy_landings <- tidyr::tibble(
    `_id` = "123",
    `_attachments.0.download_url` = "http://url-1.com",
    `_attachments.0.id` = "01",
    `_attachments.1.download_url` = "http://url-2.com",
    `_attachments.1.id` = "02",
    `_attachments.2.download_url` = NA,
    `_attachments.2.id` = NA
  )

  nested <- pt_nest_attachments(dummy_landings)

  expect_type(nested$`_attachments`, "list")
  expect_s3_class(nested$`_attachments`[[1]], "data.frame")
  expect_equal(nrow(nested$`_attachments`[[1]]), 2)
})

test_that("species are being nested properly", {
  dummy_landings <- tidyr::tibble(
    `_id` = "123",
    `species_group.0.species_group/species` = "sp01",
    `species_group.0.species_group/food_or_sale` = "food",
    `species_group.0.species_group/no_fish_by_length_group/no_individuals_10_15` = 10,
    `species_group.0.species_group/no_fish_by_length_group/no_individuals_5_10` = 10,
    `species_group.0.species_group/no_fish_by_length_group/fish_length_over60` = 80,
    `species_group.0.species_group/no_fish_by_length_group/no_individuals_over60` = 1,
    `species_group.1.species_group/species` = "sp02",
    `species_group.1.species_group/food_or_sale` = "sale",
    `species_group.1.species_group/no_fish_by_length_group/no_individuals_20_25` = 5,
    `species_group.1.species_group/no_fish_by_length_group/no_individuals_5_10` = 20,
    `species_group.1.species_group/no_fish_by_length_group/fish_length_over60` = 70,
    `species_group.1.species_group/no_fish_by_length_group/no_individuals_over60` = 2
  )

  nested <- pt_nest_species(dummy_landings)

  # Check that the form is correct
  expect_type(nested$`species_group`, "list")
  expect_s3_class(nested$`species_group`[[1]], "data.frame")
  expect_equal(nrow(nested$`species_group`[[1]]), 2)
  expect_type(nested$`species_group`[[1]]$length_individuals, "list")
  expect_s3_class(nested$`species_group`[[1]]$length_individuals[[1]], "data.frame")

  # Check that things go in the right place
  length_sp_2 <- nested$`species_group`[[1]]$length_individuals[[2]]
  expect_equal(min(length_sp_2$mean_length), 7.5)
  expect_equal(length_sp_2[length_sp_2$mean_length == 7.5, ]$n_individuals, 20)
})
