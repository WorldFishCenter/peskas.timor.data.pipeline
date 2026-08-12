# Portal contract gate — structural and numeric diff of two `portal-*.json` sets
#
# Written for migration Phase 8, reusable by Phase 11's cutover.
#
# The portal contract is *discovery-based*: `peskas.timor.portal.v2` lists
# `gs://public-timor` for the `portal-` prefix and keeps the newest version of
# each name (AUDIT §3). A renamed or dropped object does not fail a build — it
# silently disappears from the live site. So the gate asserts on **names**
# first, then on keys, nesting and types, and only then on numbers.
#
# Two baselines, and they answer different questions:
#
#   structural  vs `reference/2026-07-31_90ede9a/public-timor/` — the Phase 0
#               golden, from production run 90ede9a. This is the frozen shape.
#   numeric     vs the newest `portal-*` set in `public-timor-dev`. The golden
#               predates commit `a2c2881`'s deliberate -15.4% weight rewrite,
#               Phase 4's removal of 104,709 phantom no-catch rows and Phase 7's
#               trip-population change, so it *should* differ numerically. Use
#               it only as an order-of-magnitude band.
#
# Usage:
#   Rscript data-raw/compare-portal-json.R <baseline-dir> <candidate-dir>
#   Rscript data-raw/compare-portal-json.R <baseline-dir> <candidate-dir> --structure-only
#
# Both directories hold versioned `portal-<name>__<ts>_<sha>__.json` files; the
# version string is stripped before matching. Exits non-zero on any structural
# difference, and prints — but does not fail on — numeric ones, which always
# need a human to accept or reject them.

suppressPackageStartupMessages(library(jsonlite))

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) stop("usage: compare-portal-json.R <baseline-dir> <candidate-dir> [--structure-only]")
baseline_dir <- args[1]
candidate_dir <- args[2]
structure_only <- "--structure-only" %in% args

read_set <- function(dir) {
  files <- list.files(dir, pattern = "^portal-.*\\.json$", full.names = TRUE)
  if (!length(files)) stop("no portal-*.json found in ", dir)
  names(files) <- sub("__.*$", "", basename(files))
  # Keep the newest version of each name, as the portal does.
  ts <- sub("^.*__([0-9]{14})_.*$", "\\1", basename(files))
  keep <- vapply(split(seq_along(files), names(files)), function(i) i[which.max(ts[i])], 1L)
  lapply(files[keep], jsonlite::fromJSON, simplifyVector = TRUE)
}

# A canonical, comparable description of one node: data frames become
# "df:<nrow>x<col<type>,...>", lists recurse, atomics become "<type>[<length>]".
shape <- function(x, path, out = new.env(parent = emptyenv())) {
  if (is.data.frame(x)) {
    cols <- paste0(names(x), "<", vapply(x, function(c) class(c)[1], ""), ">")
    out[[path]] <- sprintf("df %d x %d: %s", nrow(x), ncol(x), paste(cols, collapse = ", "))
  } else if (is.list(x)) {
    out[[path]] <- sprintf("list[%d]: %s", length(x), paste(names(x), collapse = ","))
    for (i in seq_along(x)) {
      nm <- if (!is.null(names(x)) && nzchar(names(x)[i])) names(x)[i] else i
      shape(x[[i]], paste0(path, "$", nm), out)
    }
  } else {
    out[[path]] <- sprintf("%s[%d]", class(x)[1], length(x))
  }
  out
}

flatten_shape <- function(set) {
  e <- new.env(parent = emptyenv())
  for (nm in names(set)) shape(set[[nm]], nm, e)
  as.list(e)[order(names(as.list(e)))]
}

# Numeric fingerprint of every numeric column of every data frame in the set.
stats_of <- function(x, path, out = new.env(parent = emptyenv())) {
  if (is.data.frame(x)) {
    for (cn in names(x)) {
      col <- x[[cn]]
      if (is.numeric(col)) {
        out[[paste0(path, "$", cn)]] <- c(
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
      stats_of(x[[i]], paste0(path, "$", nm), out)
    }
  }
  out
}

flatten_stats <- function(set) {
  e <- new.env(parent = emptyenv())
  for (nm in names(set)) stats_of(set[[nm]], nm, e)
  as.list(e)[order(names(as.list(e)))]
}

base <- read_set(baseline_dir)
cand <- read_set(candidate_dir)

failures <- 0L
fail <- function(...) {
  cat("FAIL ", ..., "\n", sep = "")
  failures <<- failures + 1L
}

cat("baseline :", baseline_dir, "-", length(base), "objects\n")
cat("candidate:", candidate_dir, "-", length(cand), "objects\n\n")

# 1. object names -----------------------------------------------------------
cat("== 1. object names\n")
missing <- setdiff(names(base), names(cand))
added <- setdiff(names(cand), names(base))
if (length(missing)) fail("objects present in baseline and MISSING from candidate: ", paste(missing, collapse = ", "))
if (length(added)) cat("NOTE object names added by the candidate: ", paste(added, collapse = ", "), "\n", sep = "")
if (!length(missing) && !length(added)) cat("ok - same ", length(base), " names\n", sep = "")
cat("\n")

# 2. structure --------------------------------------------------------------
cat("== 2. structure (keys, nesting, column names, column types, row counts)\n")
sb <- flatten_shape(base[intersect(names(base), names(cand))])
sc <- flatten_shape(cand[intersect(names(base), names(cand))])
for (k in union(names(sb), names(sc))) {
  b <- sb[[k]]
  c_ <- sc[[k]]
  if (is.null(b)) {
    cat("NOTE node added   ", k, ": ", c_, "\n", sep = "")
    next
  }
  if (is.null(c_)) {
    fail("node dropped  ", k, ": ", b)
    next
  }
  if (identical(b, c_)) next
  # Row counts are data, not structure. Column order is not structure either:
  # the portal reads an array of objects by key name. Both are reported, but
  # only a changed column *set* or *type* is a contract break.
  strip_rows <- function(s) sub("^df [0-9]+ x ", "df N x ", sub("^list\\[[0-9]+\\]", "list[N]", s))
  sorted_cols <- function(s) {
    if (!grepl("^df ", s)) return(s)
    paste0(sub("^(df N x [0-9]+): .*$", "\\1: ", strip_rows(s)),
           paste(sort(strsplit(sub("^df N x [0-9]+: ", "", strip_rows(s)), ", ")[[1]]), collapse = ", "))
  }
  if (identical(strip_rows(b), strip_rows(c_))) {
    cat("NOTE size only    ", k, ": ", sub("^(df [0-9]+ x [0-9]+|list\\[[0-9]+\\]).*$", "\\1", b),
        " -> ", sub("^(df [0-9]+ x [0-9]+|list\\[[0-9]+\\]).*$", "\\1", c_), "\n", sep = "")
  } else if (identical(sorted_cols(b), sorted_cols(c_))) {
    cat("NOTE column order ", k, " (same column set and types; the portal reads by key)\n",
        "      baseline : ", b, "\n      candidate: ", c_, "\n", sep = "")
  } else {
    fail("shape changed ", k, "\n      baseline : ", b, "\n      candidate: ", c_)
  }
}
cat("\n")

# 3. numbers ----------------------------------------------------------------
if (!structure_only) {
  cat("== 3. per-column numeric summary (sum / mean / min / max)\n")
  nb <- flatten_stats(base[intersect(names(base), names(cand))])
  nc <- flatten_stats(cand[intersect(names(base), names(cand))])
  moved <- 0L
  for (k in intersect(names(nb), names(nc))) {
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
  cat("  ", moved, " of ", length(intersect(names(nb), names(nc))),
      " numeric columns moved by more than 1e-9 relative\n", sep = "")
  cat("\n")
}

cat("== result: ", failures, " structural failure(s)\n", sep = "")
quit(status = if (failures > 0L) 1L else 0L)
