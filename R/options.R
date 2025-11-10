#' Get SQL Server Details
#'
#' This function retrieves the stored SQL server details from the session options.
#'
#' @return A tibble with the server details.
#' @export
ezql_details <- function() {
  getOption("server_details")
}

#' Get SQL Server Address
#'
#' This function retrieves the stored SQL server address from the session options.
#'
#' @return A character string representing the server address.
#' @export
ezql_details_add <- function() {
  ezql_details()$address
}

#' Get SQL Database Name
#'
#' This function retrieves the stored SQL database name from the session options.
#'
#' @return A character string representing the database name.
#' @export
ezql_details_db <- function() {
  ezql_details()$database
}

#' Get SQL Schema Name
#'
#' This function retrieves the stored SQL schema name from the session options.
#'
#' @return A character string representing the schema name.
#' @export
ezql_details_schema <- function() {
  ezql_details()$schema
}

#' Add SQL Server Details to .Rprofile
#'
#' This function adds the SQL server details to the .Rprofile file.
#'
#' @param address The server address.
#' @param database The database name.
#' @param schema The schema name.
#'
#' @importFrom stringr str_c str_subset
#' @importFrom readr write_lines read_lines
#' @importFrom tibble tibble
#' @export
ezql_details_to_rprofile <- function(schema, database, address) {
  # Construct the path to .Rprofile
  rprofile_path <- path.expand("~") %>% file.path(".Rprofile")

  # Text to add to .Rprofile
  text_to_add <- stringr::str_c(
    "options(server_details = tibble::tibble(",
    "address = '", address, "', ",
    "database = '", database, "', ",
    "schema = '", schema, "'))"
  )

  # Check if .Rprofile exists
  file_exists <- file.exists(rprofile_path)

  tryCatch({
    if (!file_exists) {
      # Create .Rprofile if it doesn't exist
      readr::write_lines(text_to_add, file = rprofile_path)
    } else {
      # Read existing .Rprofile and update it
      current_lines <- readr::read_lines(rprofile_path)
      updated_lines <- c(
        current_lines %>% stringr::str_subset("server_details", negate = TRUE),
        text_to_add
      )
      readr::write_lines(updated_lines, file = rprofile_path)
    }
    warning("Changes to your .Rprofile do not alter the options for the current session unless you restart your R session. Do this with Ctrl+Shift+F10.")
  }, error = function(e) {
    stop("Failed to update .Rprofile: ", e$message)
  })
}

`%||%` <- function(a, b) if (!is.null(a)) a else b



#' Set SQL Server Details
#'
#' This function sets the SQL server details in the session options.
#'
#' @param address The server address (default: NULL).
#' @param database The database name (default: NULL).
#' @param schema The schema name (default: NULL).
#'
#' @importFrom stringr str_c
#' @importFrom tibble tibble
#' @importFrom purrr map_lgl map_int
#' @export
ezql_set_details <- function(schema = NULL, database = NULL, address = NULL) {

  server_details <- ezql_details()

  if ((is.null(address) || is.null(database) || is.null(schema)) && is.null(server_details)) {
    stop("No server details stored in this session's options. Please provide values for address, database, and schema.")
  }

  address <- address %||% server_details$address
  database <- database %||% server_details$database
  schema <- schema %||% server_details$schema

  if (!all(purrr::map_lgl(list(address, database, schema), is.character)) ||
      any(purrr::map_int(list(address, database, schema), length) != 1)) {
    stop("All arguments must be character vectors of length 1.")
  }

  options(server_details = tibble::tibble(address = address, database = database, schema = schema))

  message("New server details are set in this session's options as follows:")
  print(ezql_details())
}

#' Add Logging Path to .Rprofile
#'
#' This function writes the logging file path to the user's .Rprofile file,
#' enabling persistent logging configuration across R sessions.
#' It will *not* overwrite an existing path unless explicitly allowed.
#'
#' @param log_path A string specifying the full path to the log file.
#' @param overwrite Logical; whether to overwrite an existing log path entry in
#'   the .Rprofile file. Defaults to `FALSE`.
#'
#' @importFrom stringr str_subset str_c
#' @importFrom readr write_lines read_lines
#' @export
ezql_log_path_to_rprofile <- function(log_path, overwrite = FALSE) {
  # Construct the path to .Rprofile
  rprofile_path <- path.expand("~") %>% file.path(".Rprofile")

  # Text to add to .Rprofile
  text_to_add <- stringr::str_c("options(ezql.log_path = '", log_path, "')")

  # Check if .Rprofile exists
  file_exists <- file.exists(rprofile_path)

  tryCatch({
    if (!file_exists) {
      # Create .Rprofile if it doesn't exist
      readr::write_lines(text_to_add, file = rprofile_path)
    } else {
      current_lines <- readr::read_lines(rprofile_path)
      existing_line <- stringr::str_subset(current_lines, "ezql\\.log_path")

      if (length(existing_line) > 0 && !overwrite) {
        stop(
          "An existing ezql.log_path entry was found in .Rprofile.\n",
          "To replace it, re-run with overwrite = TRUE."
        )
      }

      updated_lines <- c(
        current_lines %>% stringr::str_subset("ezql\\.log_path", negate = TRUE),
        text_to_add
      )
      readr::write_lines(updated_lines, file = rprofile_path)
    }

    warning(
      "Logging path saved to .Rprofile. Restart your R session (Ctrl+Shift+F10) to apply."
    )
  }, error = function(e) {
    stop("Failed to update .Rprofile: ", e$message)
  })
}


#' Retrieve the Configured Logging Path
#'
#' This function retrieves the current logging path set in R options.
#' If no logging path has been defined (e.g., via \code{ezql_log_path_to_rprofile()}),
#' it returns \code{NULL} and optionally warns the user.
#'
#' @param warn Logical; whether to issue a warning if the logging path is not set.
#'   Defaults to \code{TRUE}.
#'
#' @return A character string containing the logging path, or \code{NULL} if none exists.
#'
#' @examples
#' \dontrun{
#' ezql_log_path()
#' }
#'
#' @export
ezql_log_path <- function(warn = TRUE) {
  path <- getOption("ezql.log_path", default = NULL)

  if (is.null(path) && warn) {
    warning("No logging path is currently set. Use ezql_log_path_to_rprofile() to configure one.")
  }

  return(path)
}

