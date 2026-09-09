#' Check the portal JSON contract
#'
#' The portal contract is *discovery-based*: `peskas.timor.portal.v2` lists
#' `gs://public-timor` for the `portal-` prefix and keeps the newest version of
#' each name. A renamed object or a dropped key does not fail a build — it
#' silently disappears from the live site. So this asserts on **names** first,
#' then on keys, nesting, column sets and column types.
#'
#' Row counts and column order are reported, never failed: the portal reads an
#' array of objects by key, and row counts are data. Numeric summaries are
#' printed only when `numbers = TRUE`, and never fail — accepting or rejecting a
#' moved number needs a human.
#'
#' `inst/tinytest/test_portal_contract.R` runs this against the frozen shape on
#' every pipeline run.
#'
#' @section Limitation:
#' A bucket set is the newest version of each name, which is what the portal
#' sees — so if [export_files()] stopped writing an object, the previous version
#' stays newest and the name check cannot fire. That is why dropping
#' `indicators_grid` was harmless. Comparing two local directories catches it.
#'
#' @param baseline,candidate Each a directory of `portal-*.json` files, a bucket
#'   (`gs://<bucket>` for the newest set, `gs://<bucket>@<version>` to pin one by
#'   any substring of the `<timestamp>_<sha>` version — pin the **full** version,
#'   since a bare sha is shared by every run of that commit), or `"latest"`, the
#'   newest set in whichever public bucket the active config resolves.
#' @param allow_dropped Object names whose removal is intended, so a missing
#'   object is a note rather than a failure.
#' @param numbers Also print a per-column numeric summary.
#' @param conf The configuration file. Read when a bucket has to be resolved.
#'
#' @return A character vector of structural failures, empty when the contract
#'   holds. The full report is printed.
#' @keywords export
#' @export
check_portal_contract <- function(baseline,
                                  candidate,
                                  allow_dropped = character(),
                                  numbers = FALSE,
                                  conf = NULL) {
  base <- read_portal_set(resolve_portal_set(baseline, conf))
  cand <- read_portal_set(resolve_portal_set(candidate, conf))

  failures <- character()
  fail <- function(...) {
    msg <- paste0(...)
    cat("FAIL ", msg, "\n", sep = "")
    failures <<- c(failures, msg)
  }

  cat("baseline :", length(base), "objects\ncandidate:", length(cand), "objects\n\n")

  cat("== 1. object names\n")
  missing <- setdiff(names(base), names(cand))
  added <- setdiff(names(cand), names(base))
  declared <- intersect(missing, allow_dropped)
  missing <- setdiff(missing, allow_dropped)
  if (length(declared)) {
    cat("NOTE dropped by declaration: ", paste(declared, collapse = ", "), "\n", sep = "")
  }
  if (length(missing)) {
    fail("objects present in baseline and MISSING from candidate: ",
         paste(missing, collapse = ", "))
  }
  if (length(added)) {
    cat("NOTE object names added by the candidate: ", paste(added, collapse = ", "), "\n", sep = "")
  }
  if (!length(missing) && !length(added)) {
    cat("ok - ", length(intersect(names(base), names(cand))), " names carried over",
        if (length(declared)) paste0(", ", length(declared), " dropped by declaration") else "",
        "\n", sep = "")
  }
  cat("\n== 2. structure (keys, nesting, column names, column types, row counts)\n")

  shared <- intersect(names(base), names(cand))
  sb <- portal_shapes(base[shared])
  sc <- portal_shapes(cand[shared])
  # Row counts are data, not structure. Column order is not structure either:
  # the portal reads an array of objects by key name.
  strip_rows <- function(s) {
    sub("^df [0-9]+ x ", "df N x ", sub("^list\\[[0-9]+\\]", "list[N]", s))
  }
  sorted_cols <- function(s) {
    if (!grepl("^df ", s)) return(s)
    paste0(sub("^(df N x [0-9]+): .*$", "\\1: ", strip_rows(s)),
           paste(sort(strsplit(sub("^df N x [0-9]+: ", "", strip_rows(s)), ", ")[[1]]),
                 collapse = ", "))
  }
  size_of <- function(s) sub("^(df [0-9]+ x [0-9]+|list\\[[0-9]+\\]).*$", "\\1", s)

  for (k in union(names(sb), names(sc))) {
    b <- sb[[k]]
    c_ <- sc[[k]]
    if (is.null(b)) {
      cat("NOTE node added   ", k, ": ", c_, "\n", sep = "")
    } else if (is.null(c_)) {
      fail("node dropped  ", k, ": ", b)
    } else if (identical(b, c_)) {
      next
    } else if (identical(strip_rows(b), strip_rows(c_))) {
      cat("NOTE size only    ", k, ": ", size_of(b), " -> ", size_of(c_), "\n", sep = "")
    } else if (identical(sorted_cols(b), sorted_cols(c_))) {
      cat("NOTE column order ", k, " (same column set and types; the portal reads by key)\n",
          "      baseline : ", b, "\n      candidate: ", c_, "\n", sep = "")
    } else {
      fail("shape changed ", k, "\n      baseline : ", b, "\n      candidate: ", c_)
    }
  }

  if (isTRUE(numbers)) report_portal_numbers(base[shared], cand[shared])

  cat("\n== result: ", length(failures), " structural failure(s)\n", sep = "")
  failures
}

# `gs://<bucket>[@<version>]` and `latest` are downloaded to a temp directory;
# anything else is taken as a local path, so no golden copy has to be kept.
resolve_portal_set <- function(spec, conf = NULL) {
  if (!identical(spec, "latest") && !grepl("^gs://", spec)) {
    return(spec)
  }
  version <- if (grepl("@", spec)) sub("^.*@", "", spec) else NULL
  conf <- conf %||% read_config()
  opts <- conf$public_storage$google$options
  if (!identical(spec, "latest")) {
    opts <- utils::modifyList(opts, list(bucket = sub("^gs://([^@]+).*$", "\\1", spec)))
  }
  key <- conf$public_storage$google$key
  coasts::cloud_storage_authenticate(provider = key, options = opts)
  names_all <- coasts::cloud_object_names(
    prefix = "portal-", provider = key, options = opts, latest_only = is.null(version)
  )
  if (!is.null(version)) {
    names_all <- grep(version, names_all, fixed = TRUE, value = TRUE)
  }
  if (!length(names_all)) stop("no portal-*.json matched in ", spec)
  # A version substring may be a commit sha, which many runs share. Reduce to
  # the newest version of each name before downloading, as the portal does.
  names_all <- newest_per_object(names_all)
  dir <- file.path(tempdir(), paste0("portal-", sub("[^A-Za-z0-9]+", "-", sub("^gs://", "", spec))))
  dir.create(dir, showWarnings = FALSE, recursive = TRUE)
  logger::log_info("Fetching {length(names_all)} objects from {spec}")
  coasts::download_cloud_file(
    name = names_all, provider = key, options = opts,
    file = file.path(dir, names_all)
  )
  dir
}

# Keep the newest version of each object name, as the portal does.
newest_per_object <- function(x) {
  obj <- sub("__.*$", "", basename(x))
  ts <- sub("^.*__([0-9]{14})_.*$", "\\1", basename(x))
  x[vapply(split(seq_along(x), obj), function(i) i[which.max(ts[i])], 1L)]
}

read_portal_set <- function(dir) {
  files <- list.files(dir, pattern = "^portal-.*\\.json$", full.names = TRUE)
  if (!length(files)) stop("no portal-*.json found in ", dir)
  files <- newest_per_object(files)
  names(files) <- sub("__.*$", "", basename(files))
  # Name the file that failed. An unauthenticated download writes the API's
  # error body to disk, and `fromJSON()` then reports a bare "premature EOF".
  lapply(files, function(f) {
    tryCatch(
      jsonlite::fromJSON(f, simplifyVector = TRUE),
      error = function(e) {
        stop(basename(f), " (", file.size(f), " bytes) is not valid JSON: ",
             conditionMessage(e), call. = FALSE)
      }
    )
  })
}

# A canonical, comparable description of one node: data frames become
# "df <nrow> x <ncol>: <col<type>, ...>", lists recurse, atomics become
# "<type>[<length>]".
portal_shapes <- function(set) {
  e <- new.env(parent = emptyenv())
  walk <- function(x, path) {
    if (is.data.frame(x)) {
      cols <- paste0(names(x), "<", vapply(x, function(cl) class(cl)[1], ""), ">")
      e[[path]] <- sprintf("df %d x %d: %s", nrow(x), ncol(x), paste(cols, collapse = ", "))
    } else if (is.list(x)) {
      e[[path]] <- sprintf("list[%d]: %s", length(x), paste(names(x), collapse = ","))
      for (i in seq_along(x)) {
        nm <- if (!is.null(names(x)) && nzchar(names(x)[i])) names(x)[i] else i
        walk(x[[i]], paste0(path, "$", nm))
      }
    } else {
      e[[path]] <- sprintf("%s[%d]", class(x)[1], length(x))
    }
  }
  for (nm in names(set)) walk(set[[nm]], nm)
  out <- as.list(e)
  out[order(names(out))]
}

# Numeric fingerprint of every numeric column of every data frame in the set.
portal_stats <- function(set) {
  e <- new.env(parent = emptyenv())
  walk <- function(x, path) {
    if (is.data.frame(x)) {
      for (cn in names(x)) {
        col <- x[[cn]]
        if (is.numeric(col)) {
          e[[paste0(path, "$", cn)]] <- c(
            n = length(col), na = sum(is.na(col)),
            sum = sum(col, na.rm = TRUE), mean = mean(col, na.rm = TRUE),
            min = suppressWarnings(min(col, na.rm = TRUE)),
            max = suppressWarnings(max(col, na.rm = TRUE))
          )
        }
      }
    } else if (is.list(x)) {
      for (i in seq_along(x)) {
        nm <- if (!is.null(names(x)) && nzchar(names(x)[i])) names(x)[i] else i
        walk(x[[i]], paste0(path, "$", nm))
      }
    }
  }
  for (nm in names(set)) walk(set[[nm]], nm)
  out <- as.list(e)
  out[order(names(out))]
}

report_portal_numbers <- function(base, cand) {
  cat("\n== 3. per-column numeric summary (sum / mean / min / max)\n")
  nb <- portal_stats(base)
  nc <- portal_stats(cand)
  shared <- intersect(names(nb), names(nc))
  moved <- 0L
  for (k in shared) {
    b <- nb[[k]]
    c_ <- nc[[k]]
    rel <- ifelse(b == 0, ifelse(c_ == 0, 0, Inf), (c_ - b) / abs(b))
    if (any(abs(rel[c("sum", "mean", "min", "max")]) > 1e-9, na.rm = TRUE)) {
      moved <- moved + 1L
      cat(sprintf(
        "  %-58s n %6d->%-6d sum %+.4f%%  mean %+.4f%%  min %+.4f%%  max %+.4f%%\n",
        k, b[["n"]], c_[["n"]], 100 * rel[["sum"]], 100 * rel[["mean"]],
        100 * rel[["min"]], 100 * rel[["max"]]
      ))
    }
  }
  cat("  ", moved, " of ", length(shared),
      " numeric columns moved by more than 1e-9 relative\n", sep = "")
}
