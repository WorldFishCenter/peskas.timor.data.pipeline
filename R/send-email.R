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

  file <- list.files(
    system.file("report/enumerators_summary",
                package = "peskas.timor.data.pipeline"),
    pattern = ".pdf"
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

  email %>%
    blastula::smtp_send(
      from = "peskas.portal@gmail.com",
      to = "lorenzo.longobardi@gmail.com",
      subject = "Monthly Enumerator Activity Report",
      credentials = blastula::creds_key(id = pars$peskas_mail$key)
    )
}
