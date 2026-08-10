dummy <- tidyr::tibble(
  `_id` = c("1", "2"),
  `species_group.0.species_group/species` = c("3", "0"),
  `species_group.0.species_group/food_or_sale` = c("food", NA),
  `species_group.0.species_group/no_fish_by_length_group/no_individuals_5_10` =
    c("4", NA),
  `species_group.0.species_group/no_fish_by_length_group/no_individuals_over60` =
    c("2", NA),
  `species_group.0.species_group/no_fish_by_length_group/fish_length_over60` =
    c("85", NA),
  `species_group.1.species_group/species` = c("10", NA),
  `species_group.1.species_group/food_or_sale` = c("sale", NA),
  `species_group.1.species_group/no_fish_by_length_group/no_individuals_5_10` =
    c("1", NA),
  `species_group.1.species_group/no_fish_by_length_group/no_individuals_over60` =
    c(NA, NA),
  `species_group.1.species_group/no_fish_by_length_group/fish_length_over60` =
    c(NA, NA)
)

test_that("empty catch slots are dropped and the index is 1-based", {
  catches <- reshape_species_groups(dummy)

  expect_equal(catches$submission_id, c("1", "1", "2"))
  expect_equal(catches$n_catch, c(1L, 2L, 1L))
  expect_equal(catches$species, c("3", "10", "0"))
})

test_that("length bins become rows at their midpoint", {
  lf <- reshape_species_groups(dummy) %>%
    expand_length_frequency() %>%
    dplyr::filter(.data$submission_id == "1", .data$n_catch == 1)

  # The 5-10 bin is 7.5 cm; the over-60 bin takes the recorded 85 cm.
  expect_equal(lf$length, c(7.5, 85))
  expect_equal(lf$n_individuals, c(4, 2))
  # `fish_length_over60` is a measurement, not a count, so it is not a bin.
  expect_false(any(grepl("fish_length_over60", names(lf))))
})

test_that("bins with no count are kept, so the portal row count is stable", {
  lf <- reshape_species_groups(dummy) %>%
    expand_length_frequency() %>%
    dplyr::filter(.data$submission_id == "1", .data$n_catch == 2)

  expect_equal(nrow(lf), 2)
  expect_equal(lf$n_individuals, c(1, NA))
})
