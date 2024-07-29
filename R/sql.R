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

# This function modifies 'sqlwrite' function in RODBC package so that it can work with temporal tables
modify_rodbc_to_work_with_temporal <- function() {

  custom_sqlwrite <- function (channel, tablename, mydata, test = FALSE, fast = TRUE,
                               nastring = NULL, verbose = FALSE)
  {
    if (!odbcValidChannel(channel))
      stop("first argument is not an open RODBC channel")
    colnames <- as.character(sqlColumns(channel, tablename)[4L][,1L])

    #CUSTOM CHANGE START: REMOVE AUTO-GENERATED COLUMNS!
    auto_generated_colindices <- which(!is.na(sqlColumns(channel, tablename)[13L][,1L]))
    if (length(auto_generated_colindices)>0) {
      colnames <- colnames[-auto_generated_colindices]
    }
    #CUSTOM CHANGE END

    colnames <- mangleColNames(colnames)
    cnames <- paste(quoteColNames(channel, colnames), collapse = ", ")
    dbname <- quoteTabNames(channel, tablename)
    if (!fast) {
      for (i in seq_along(mydata)) if (is.logical(mydata[[i]]))
        mydata[[i]] <- as.character(mydata[[i]])
      data <- as.matrix(mydata)
      if (nchar(enc <- attr(channel, "encoding")) && is.character(data))
        data <- iconv(data, to = enc)
      colnames(data) <- colnames
      cdata <- sub("\\([[:digit:]]*\\)", "", sqlColumns(channel,
                                                        tablename)[, "DATA_TYPE"])
      tdata <- sqlTypeInfo(channel)
      nr <- match(cdata, tdata[, 2L])
      tdata <- as.matrix(tdata[nr, 4:5])
      tdata[is.na(nr), ] <- "'"
      for (cn in seq_along(cdata)) {
        td <- as.vector(tdata[cn, ])
        if (is.na(td[1L]))
          next
        if (identical(td, rep("'", 2L)))
          data[, cn] <- gsub("'", "''", data[, cn])
        data[, cn] <- paste(td[1L], data[, cn], td[2L],
                            sep = "")
      }
      data[is.na(mydata)] <- if (is.null(nastring))
        "NULL"
      else nastring[1L]
      for (i in 1L:nrow(data)) {
        query <- paste("INSERT INTO", dbname, "(", cnames,
                       ") VALUES (", paste(data[i, colnames], collapse = ", "),
                       ")")
        if (verbose)
          cat("Query: ", query, "\n", sep = "")
        if (odbcQuery(channel, query) < 0)
          return(-1L)
      }
    }
    else {
      query <- paste("INSERT INTO", dbname, "(", cnames, ") VALUES (",
                     paste(rep("?", ncol(mydata)), collapse = ","), ")")
      if (verbose)
        cat("Query: ", query, "\n", sep = "")
      coldata <- sqlColumns(channel, tablename)[c(4L, 5L,
                                                  7L, 9L)]
      if (any(is.na(m <- match(colnames, coldata[, 1]))))
        return(-1L)
      if (any(notOK <- (coldata[, 3L] == 0L))) {
        types <- coldata[notOK, 2]
        tdata <- sqlTypeInfo(channel)
        coldata[notOK, 3L] <- tdata[match(types, tdata[,
                                                       2L]), 3L]
      }
      if (odbcUpdate(channel, query, mydata, coldata[m, ],
                     test = test, verbose = verbose, nastring = nastring) <
          0)
        return(-1L)
    }
    return(invisible(1L))
  }

  environment(custom_sqlwrite) <- asNamespace('RODBC')
  assignInNamespace("sqlwrite", custom_sqlwrite, ns = "RODBC")

}


add_data_to_table_on_server <- function(data, sql_table_name, interface_column_names = TRUE) {

  modify_rodbc_to_work_with_temporal()

  #Can we change this if statement to interface_column_names == FALSE? The code
  #as written works fine, obvi, but takes a few seconds to understand.
  if (!interface_column_names) { #if engine column names used, then do clean-up first
    data <-
      data %>%
      #In the line below, is there a reason to say !interface_column_names
      #rather than FALSE?
      clean_up_data_types(engine_column_names = !interface_column_names) %>%
      rename_with(.fn = get_interface_column_names)
  }

  #connect
  connection <- connect_to_server()


  #check if table exists:
  if (!(sql_table_name %in% sqlTables(connection)$TABLE_NAME)) {
    stop ('This table does not exist on the SQL server.')
  }

  #prepend schema
  full_table_name <- str_c(get_server_details()$Schema, '.', sql_table_name)

  #add the data
  sqlSave(connection, data, full_table_name, append = TRUE, rownames = FALSE, verbose = TRUE)

  #close connection
  odbcClose(connection)

}
