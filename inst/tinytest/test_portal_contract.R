library(peskas.timor.data.pipeline)

logger::log_threshold(logger::ERROR)
# Local runs read their credentials from `.env`; CI supplies them as real
# environment variables, where this is a no-op.
if (file.exists(".env")) dotenv::load_dot_env()

# The frozen shape: production run 90ede9a of 2026-07-31. Pin the FULL version —
# a bare sha is shared by every run of that commit. `latest` is the newest set in
# whichever public bucket the active config resolves, so this works unchanged
# against dev and production.
#
# `indicators_grid` and `label_groups_list` are in the golden set and are no
# longer written; the portal excludes both. Any other missing object is a
# contract break.
failures <- peskas.timor.data.pipeline::check_portal_contract(
  baseline = "gs://public-timor@20260731031516_90ede9a",
  candidate = "latest",
  allow_dropped = c("portal-indicators_grid", "portal-label_groups_list")
)

expect_true(
  length(failures) == 0,
  info = paste0(length(failures), " structural failure(s): ",
                paste(failures, collapse = " | "))
)
