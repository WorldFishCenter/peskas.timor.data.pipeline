#' Add timestamp and sha string to a file name
#'
#' An alternative to version data is to name it using the sha (unique
#' identifier) of the code using to generate or process the data and the time at
#' which the data was generated or processed. This function adds this
#' information, a version identifier, to a file name (character string)
#'
#' @param filename Path sans extension of the file to version
#' @param extension Extension of the file
#' @param sha_nchar Number of characters from the SHA to use as the version
#'   identifier
#' @param sep Characters separating the version identifier from the file name
#'
#' @return A character string with the file name and the version identifier
#' @export
#'
#' @details
#'
#' The SHA information is retrieved using [git2r::sha]. If the code is not
#' running in a context aware of a git repository (for example when code is
#' running inside a container) then this function attempts to get the sha from
#' the environment variable `GITHUB_SHA`. If both of these methods fail, no sha
#' versioning is added.
#'
#' @keywords helper
#' @examples
#' if (git2r::in_repository()) {
#'   add_version("my_file", "csv")
#' }
add_version <- function(filename, extension = "", sha_nchar = 7, sep = "__") {
  # Git sha are 40 characters long
  stopifnot(sha_nchar <= 40)

  version <- format(Sys.time(), "%Y%m%d%H%M%S")

  if (git2r::in_repository()) {
    commit_sha <- substr(git2r::sha(git2r::last_commit()), 1, sha_nchar)
    version <- paste(version, commit_sha, sep = "_")
  } else if (Sys.getenv("GITHUB_SHA") != "") {
    # If not in a git repository (for example when code is running inside a
    # container) get the sha from an environment variable if available
    commit_sha <- substr(Sys.getenv("GITHUB_SHA"), 1, sha_nchar)
    version <- paste(version, commit_sha, sep = "_")
  }

  # If the extension comes without dot, add one
  if (nchar(extension) > 0 & substr(extension, 1, 1) != ".") {
    extension <- paste0(".", extension)
  }

  paste0(filename, sep, version, sep, extension)
}


#' Load environment variables from a .env file
#'
#' Loads environment variables from a `.env` file if one exists. Called by
#' [read_config()] before the configuration is resolved, so that the
#' `!expr Sys.getenv(...)` entries in `inst/config.yml` see the local secrets.
#'
#' In CI the variables come from the workflow environment and no `.env` file is
#' present, so this is a no-op there. Values already set in the environment win:
#' [dotenv::load_dot_env()] does not overwrite them.
#'
#' @param file Path to the `.env` file. Defaults to `.env` in the working
#'   directory.
#'
#' @return `NULL`, invisibly. Called for its side effect.
#'
#' @keywords helper
#' @export
#'
load_dotenv <- function(file = ".env") {
  if (file.exists(file)) {
    logger::log_info("Loading environment variables from {file}")
    dotenv::load_dot_env(file = file)
  } else {
    logger::log_debug("No .env file found at {file}, skipping dotenv loading")
  }
  invisible(NULL)
}

#' Read configuration file
#'
#' Reads the package configuration and adds some logging lines. Wrapped for
#' convenience.
#'
#' Environment variables are loaded from `.env` first (see [load_dotenv()]), so
#' local runs and CI resolve the same configuration branch and differ only by
#' `R_CONFIG_ACTIVE`.
#'
#' The configuration file is `inst/config.yml`. `conf.yml` is still accepted as
#' a fallback so that an older installed copy of the package keeps resolving.
#'
#' @return the environment parameters
#'
#' @keywords helper
#' @export
#'
read_config <- function() {
  # Load .env first so config.yml's Sys.getenv() entries can see the values
  load_dotenv()

  logger::log_info("Loading configuration file...")

  conf_file <- system.file(
    "config.yml",
    package = "peskas.timor.data.pipeline"
  )
  if (!nzchar(conf_file)) {
    conf_file <- system.file(
      "conf.yml",
      package = "peskas.timor.data.pipeline"
    )
  }
  if (!nzchar(conf_file)) {
    stop(
      "No 'inst/config.yml' found in package 'peskas.timor.data.pipeline'.",
      call. = FALSE
    )
  }

  conf <- config::get(
    config = Sys.getenv("R_CONFIG_ACTIVE", "default"),
    file = conf_file
  )

  logger::log_info("Using configutation: {attr(conf, 'config')}")
  # Deliberately not dumping `conf`. Workflow functions default to
  # `log_threshold = logger::DEBUG`, and the resolved config carries the
  # service-account private key, the Airtable PAT, the Dataverse token and the
  # blastula credentials. GitHub Actions only masks byte-exact matches of a
  # registered secret, which the re-serialised JSON is not.
  logger::log_debug("Configuration keys: {paste(names(conf), collapse = ', ')}")

  conf
}
