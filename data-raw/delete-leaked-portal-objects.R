# Delete the 45 leaked absolute-path objects in gs://public-timor
# =============================================================================
#
# One-off. In January 2026 `export_files()` uploaded with the *full local path*
# as the object name, so `gs://public-timor` carries 45 objects called
#
#     /tmp/Rtmph2kG06/portal-json/portal-<name>__20260118223318_0d6af65__.json
#
# note the leading slash -- they list as `gs://public-timor//tmp/...`. The bug is
# long fixed (`export_files()` passes basenames and normalises; verified in
# migration Phase 2), so these are residue, not a live fault. They are invisible
# to the portal, which lists the prefix `portal-` while these start with `/tmp/`.
#
# RUN ONLY AFTER the first green production run post-merge. If anything goes
# wrong with the cutover you want the bucket unchanged apart from the new writes.
#
# Usage -- `.Renviron` pins R_CONFIG_ACTIVE=default and overrides the command
# line, so a bare `R_CONFIG_ACTIVE=production Rscript ...` silently targets dev:
#
#   R_ENVIRON_USER=/dev/null R_CONFIG_ACTIVE=production \
#     Rscript data-raw/delete-leaked-portal-objects.R
#
# or interactively: use_prod(); source("data-raw/delete-leaked-portal-objects.R")
#
# Dry run by default. Set PESKAS_CONFIRM_DELETE=yes to actually delete.
# =============================================================================

library(peskas.timor.data.pipeline)
conf <- read_config()
opts <- conf$public_storage$google$options

stopifnot("refusing to run outside production" = opts$bucket == "public-timor")

coasts::cloud_storage_authenticate(conf$public_storage$google$key, opts)
all_objects <- googleCloudStorageR::gcs_list_objects(bucket = opts$bucket)

# Only names starting with a slash: a real portal object never does.
leaked <- all_objects$name[startsWith(all_objects$name, "/")]

message("bucket ", opts$bucket, ": ", nrow(all_objects), " objects, ",
        length(leaked), " leaked")
if (length(leaked)) {
  message(paste(utils::head(leaked, 3), collapse = "\n"))
  if (length(leaked) > 3) message("  ... and ", length(leaked) - 3, " more")
} else {
  message("nothing to do")
}

# Guard: the portal keeps the newest `portal-<name>__*` of each name. Deleting a
# leaked object must not be able to change that set.
live <- grep("^portal-", all_objects$name, value = TRUE)
stopifnot(
  "a leaked name is also a live portal name" = length(intersect(leaked, live)) == 0
)

if (!identical(Sys.getenv("PESKAS_CONFIRM_DELETE"), "yes")) {
  message("\nDRY RUN. Re-run with PESKAS_CONFIRM_DELETE=yes to delete.")
} else {
  ok <- vapply(leaked, function(nm) {
    tryCatch({
      googleCloudStorageR::gcs_delete_object(nm, bucket = opts$bucket)
      TRUE
    }, error = function(e) {
      message("FAILED ", nm, ": ", conditionMessage(e)); FALSE
    })
  }, logical(1))
  message("\ndeleted ", sum(ok), " of ", length(leaked))
  left <- googleCloudStorageR::gcs_list_objects(bucket = opts$bucket)$name
  message("remaining leaked: ", sum(startsWith(left, "/")))
}
