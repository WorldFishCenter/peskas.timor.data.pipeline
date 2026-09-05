# The taxa/weight path's two guards and the alias table.
#
# `assert_taxa_coverage()` exists because a partial FishBase fetch silently
# dropped `CJX` and `PWT` from a production coefficient table and their catch
# weighed zero (COASTS-TODO C25). A guard that has only ever been run against
# passing input is not a guard, so it is tested against the failure it was
# written for.

test_that("assert_taxa_coverage() passes when every taxon resolves", {
  taxa <- dplyr::tibble(
    alpha3_code = c("TUN", "CLP", "MZZ", "SWX"),
    scientific_name = c("Thunnus", "Clupeidae", "Actinopterygii", "Algae")
  )
  lw <- dplyr::tibble(alpha3_code = c("TUN", "CLP"), n_studies = c(10L, 20L))

  # It logs a summary line on success, so assert the return value, not silence.
  expect_identical(
    suppressMessages(
      peskas.timor.data.pipeline:::assert_taxa_coverage(taxa, lw)
    ),
    lw
  )
})

test_that("assert_taxa_coverage() fails on the CJX/PWT disappearance", {
  taxa <- dplyr::tibble(
    alpha3_code = c("TUN", "CJX", "PWT"),
    scientific_name = c("Thunnus", "Caesionidae", "Scaridae")
  )
  lw <- dplyr::tibble(alpha3_code = "TUN", n_studies = 10L)

  expect_error(
    peskas.timor.data.pipeline:::assert_taxa_coverage(taxa, lw),
    "CJX, PWT"
  )
})

test_that("MZZ and SWX are exempt, and are the only exemptions", {
  taxa <- dplyr::tibble(alpha3_code = c("MZZ", "SWX"), scientific_name = c("a", "b"))
  lw <- dplyr::tibble(alpha3_code = character(), n_studies = integer())

  expect_no_error(
    suppressMessages(peskas.timor.data.pipeline:::assert_taxa_coverage(taxa, lw))
  )

  # One more unresolved code, and it must fail.
  expect_error(
    peskas.timor.data.pipeline:::assert_taxa_coverage(
      dplyr::add_row(taxa, alpha3_code = "CLP", scientific_name = "c"),
      lw
    ),
    "CLP"
  )
})

test_that("the search aliases are additive and cover only the four known codes", {
  a <- peskas.timor.data.pipeline:::taxa_search_aliases()

  expect_setequal(unique(a$alpha3_code), c("TUN", "SKH", "LGE", "CLP"))
  # The tribe Thunnini is five genera; TUN is 56% of landed weight and pooling
  # the wrong ones moves national catch by ~10%.
  expect_setequal(
    a$scientific_name[a$alpha3_code == "TUN"],
    c("Allothunnus", "Auxis", "Euthynnus", "Katsuwonus", "Thunnus")
  )
  # CLP keeps Clupeidae from ASFIS and *adds* Dorosomatidae; if this ever
  # became a replacement, the published taxon would change.
  expect_identical(a$scientific_name[a$alpha3_code == "CLP"], "Dorosomatidae")
})

test_that("the curated coefficients ship with the package and are usable", {
  x <- peskas.timor.data.pipeline:::curated_lw_coeffs()

  expect_true(nrow(x) > 0)
  expect_named(
    x,
    c("alpha3_code", "species_found", "Type", "EsQ", "a", "b")
  )
  # `summarise_lw_coeffs()` drops rows with a missing or non-positive `a`, so a
  # hole here would silently shrink a taxon's pool rather than error.
  expect_false(anyNA(x$a))
  expect_false(anyNA(x$b))
  expect_true(all(x$a > 0))
  expect_true(all(nzchar(x$alpha3_code)))
})
