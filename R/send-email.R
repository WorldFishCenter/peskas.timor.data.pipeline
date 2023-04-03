#' Send sites report email
#'
#' This function takes advantage of the package `blastula` to send an email
#' containing a summary report in each landig site.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#' threshold for the logging infrastructure. See [logger::log_levels] for more
#' details
#'
#' @return Nothing, this function is useful for its side effects.
#' @export
#'
send_sites_report <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  month <- lubridate::month(Sys.Date(), label = T)
  year <- lubridate::year(Sys.Date())

  logger::log_info("Generate mail")
  email <-
    blastula::compose_email(
      body = blastula::md(
        glue::glue(
          "Hello,

      The attached report provides a summary of the overall performance and activity in each landing site on an annual and monthly scale.
      The report includes information on the number of surveys submitted, the working days, and the estimated hours per day in each landing site.

      We believe that this report will provide valuable insights into the enumerators work and can help to identify any areas for improvement.
      We would appreciate it if you could take the time to review the report and provide us with your feedback.

      Please note that the report is automatically generated.
      If you have any questions or concerns, please do not hesitate to contact us.

      Thank you,

      The peskAAS Team"
        )
      ),
      footer = blastula::md(glue::glue("Email sent on ", as.character(Sys.time())))
    )

  logger::log_info("Attach report to mail")

  file <- list.files(
    system.file("report", package = "peskas.timor.data.pipeline"),
    pattern = c("summary_report.pdf"), full.names = T
  )

  content_type <- mime::guess_type(file)
  filename <- basename(file)

  expanded_path <-
    file %>%
    path.expand() %>%
    normalizePath(mustWork = TRUE)

  attachment_list <- list(
    file_path = expanded_path,
    content_type = content_type,
    disposition = "attachment",
    filename = filename
  )
  email$attachments <- c(email$attachments, list(attachment_list))

  logger::log_info("Generate credentials file")

  file_cred <- file("creds.txt")
  writeLines(pars$peskas_mail$key, file_cred)
  close(file_cred)

  logger::log_info("Send mail")

  email %>%
    blastula::smtp_send(
      from = "peskas.platform@gmail.com",
      to = c("l.longobardi@cgiar.org", "lorenzo.longobardi@gmail.com"),
      subject = paste("Monthly Enumerator Activity Report", paste(month, year)),
      credentials = blastula::creds_file("creds.txt")
    )
  file.remove("creds.txt")
}

#' Send validation summary email
#'
#' This function takes advantage of the package `blastula` to send an email
#' containing a summary of the latest submissions with problems.
#'
#' @param log_threshold The (standard Apache logj4) log level used as a
#' threshold for the logging infrastructure. See [logger::log_levels] for more
#' details
#'
#' @return Nothing, this function is useful for its side effects.
#' @export
#'
send_validation_mail <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  pars <- read_config()

  validation_df <- get_validation_sheet(pars)

  last_week <-
    validation_df %>%
    dplyr::filter(.data$submission_date >= Sys.Date() - 7) %>%
    dplyr::filter(!is.na(.data$alert))

  n_submissions_alert <- nrow(last_week)

  logger::log_info("Generate mail")
  email <-
    blastula::compose_email(
      body = blastula::md(
        c(
          glue::glue(
            "Hi there,

          In the last week there have been {n_submissions_alert} new landing surveys
          that may have some problems with the data entered. Please, open the link below
          and check the submission on KoBoToolBox to make corrections.
          If you don't think the flag is an error please let us know.

          https://docs.google.com/spreadsheets/d/1MjpEE-5oOqQgpf8M8h_IysQlLdZroouOkGjy6UP95UM/edit?usp=sharing
          "
          ),
          last_week %>%
            kableExtra::kbl() %>%
            kableExtra::kable_styling()
        )
      ),
      footer = blastula::md(glue::glue("Email sent on ", as.character(Sys.time())))
    )

  logger::log_info("Generate credentials file")

  file_cred <- file("creds.txt")
  writeLines(pars$peskas_mail$key, file_cred)
  close(file_cred)

  logger::log_info("Send mail")

  email %>%
    blastula::smtp_send(
      from = "peskas.platform@gmail.com",
      to = c("l.longobardi@cgiar.org", "lorenzo.longobardi@gmail.com"),
      subject = paste("Peskas automations:", n_submissions_alert, "new submissions have problems"),
      credentials = blastula::creds_file("creds.txt")
    )
  file.remove("creds.txt")
}
