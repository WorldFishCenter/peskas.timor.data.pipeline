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
#' @param sep Characters separating the version identifier from the filename
#'
#' @return A character string with the filename and the version identifier
#' @export
#'
#' @examples
#' if (git2r::in_repository()) {
#' add_version("my_file", "csv")
#' }
#'
add_version <- function(filename, extension = "", sha_nchar = 7, sep = "__"){

  # Git sha are 40 characters long
  stopifnot(sha_nchar <= 40)

  version <- format(Sys.time(), "%Y%m%d%H%M%S")

  if (git2r::in_repository()){
    commit_sha <- substr(git2r::sha(git2r::last_commit()), 1, sha_nchar)
    version <- paste(version, commit_sha, sep = "_")
  }

  # If the extension comes without dot, add one
  if (nchar(extension) > 0 & substr(extension, 1, 1) != ".") {
    extension <- paste0(".", extension)
  }

  paste0(filename, sep, version, sep, extension)
}
