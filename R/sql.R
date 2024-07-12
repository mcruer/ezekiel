#' Connect to SQL Server
#'
#' This function creates a connection to the SQL server using the stored server details.
#'
#' @param database The name of the database to connect to. If NULL, it uses the value from `ezql_details_db()`.
#' @param address The address of the SQL server. If NULL, it uses the value from `ezql_details_add()`.
#' @return An RODBC connection object if successful; otherwise, an error is thrown.
#' @importFrom stringr str_c
#' @export
ezql_connect <- function(database = NULL, address = NULL) {
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()



  # Construct the connection string
  connection_string <- stringr::str_c(
    "driver={SQL Server};server=", address,
    ";database=", database,
    ";trusted_connection=yes;"
  )

  # Attempt to create the connection
  connection <- RODBC::odbcDriverConnect(connection_string)

  # Check if the connection is successful
  if (!is.null(connection)) {
    print('Successfully connected to server!')
  } else {
    stop('Error: Could not connect to server!')
  }


  return(connection)
}


#' Fetch Data from SQL Table
#'
#' This function fetches all data from the specified SQL table.
#'
#' @param sql_table_name The name of the SQL table.
#' @param schema The schema name (default: NULL, will use ezql_details_schema()).
#' @param database The database name (default: NULL, will use ezql_details_db()).
#' @param address The server address (default: NULL, will use ezql_details_add()).
#' @return A tibble containing the data from the specified SQL table.
#' @importFrom stringr str_c
#' @importFrom tibble as_tibble
#' @importFrom RODBC sqlQuery odbcClose
#' @export
ezql_get <- function(sql_table_name, schema = NULL, database = NULL, address = NULL) {
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided either as arguments or through ezql_details.")
  }

  full_table_name <- stringr::str_c(schema, '.', sql_table_name)
  query <- stringr::str_c('SELECT * FROM ', full_table_name)

  connection <- ezql_connect(database = database, address = address)

  data <- tryCatch({
    result <- RODBC::sqlQuery(connection, query, errors = TRUE)
    if (inherits(result, "character")) {
      stop(result)
    }
    tibble::as_tibble(result)
  }, error = function(e) {
    RODBC::odbcClose(connection)
    stop("Error executing SQL query: ", e$message)
  })

  RODBC::odbcClose(connection)
  return(data)
}
