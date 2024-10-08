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
#' @param table The name of the SQL table.
#' @param schema The schema name (default: NULL, will use ezql_details_schema()).
#' @param database The database name (default: NULL, will use ezql_details_db()).
#' @param address The server address (default: NULL, will use ezql_details_add()).
#' @return A tibble containing the data from the specified SQL table.
#' @importFrom stringr str_c
#' @importFrom tibble as_tibble
#' @importFrom RODBC sqlQuery odbcClose
#' @export
ezql_get <- function(table, schema = NULL, database = NULL, address = NULL) {
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided either as arguments or through ezql_details.")
  }

  full_table_name <- ezql_full_table_name(schema, table)
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
# modify_rodbc_to_work_with_temporal <- function() {
#
#   custom_sqlwrite <- function (channel, tablename, mydata, test = FALSE, fast = TRUE,
#                                nastring = NULL, verbose = FALSE)
#   {
#     if (!odbcValidChannel(channel))
#       stop("first argument is not an open RODBC channel")
#     colnames <- as.character(sqlColumns(channel, tablename)[4L][,1L])
#
#     #CUSTOM CHANGE START: REMOVE AUTO-GENERATED COLUMNS!
#     auto_generated_colindices <- which(!is.na(sqlColumns(channel, tablename)[13L][,1L]))
#     if (length(auto_generated_colindices)>0) {
#       colnames <- colnames[-auto_generated_colindices]
#     }
#     #CUSTOM CHANGE END
#
#     colnames <- mangleColNames(colnames)
#     cnames <- paste(quoteColNames(channel, colnames), collapse = ", ")
#     dbname <- quoteTabNames(channel, tablename)
#     if (!fast) {
#       for (i in seq_along(mydata)) if (is.logical(mydata[[i]]))
#         mydata[[i]] <- as.character(mydata[[i]])
#       data <- as.matrix(mydata)
#       if (nchar(enc <- attr(channel, "encoding")) && is.character(data))
#         data <- iconv(data, to = enc)
#       colnames(data) <- colnames
#       cdata <- sub("\\([[:digit:]]*\\)", "", sqlColumns(channel,
#                                                         tablename)[, "DATA_TYPE"])
#       tdata <- sqlTypeInfo(channel)
#       nr <- match(cdata, tdata[, 2L])
#       tdata <- as.matrix(tdata[nr, 4:5])
#       tdata[is.na(nr), ] <- "'"
#       for (cn in seq_along(cdata)) {
#         td <- as.vector(tdata[cn, ])
#         if (is.na(td[1L]))
#           next
#         if (identical(td, rep("'", 2L)))
#           data[, cn] <- gsub("'", "''", data[, cn])
#         data[, cn] <- paste(td[1L], data[, cn], td[2L],
#                             sep = "")
#       }
#       data[is.na(mydata)] <- if (is.null(nastring))
#         "NULL"
#       else nastring[1L]
#       for (i in 1L:nrow(data)) {
#         query <- paste("INSERT INTO", dbname, "(", cnames,
#                        ") VALUES (", paste(data[i, colnames], collapse = ", "),
#                        ")")
#         if (verbose)
#           cat("Query: ", query, "\n", sep = "")
#         if (odbcQuery(channel, query) < 0)
#           return(-1L)
#       }
#     }
#     else {
#       query <- paste("INSERT INTO", dbname, "(", cnames, ") VALUES (",
#                      paste(rep("?", ncol(mydata)), collapse = ","), ")")
#       if (verbose)
#         cat("Query: ", query, "\n", sep = "")
#       coldata <- sqlColumns(channel, tablename)[c(4L, 5L,
#                                                   7L, 9L)]
#       if (any(is.na(m <- match(colnames, coldata[, 1]))))
#         return(-1L)
#       if (any(notOK <- (coldata[, 3L] == 0L))) {
#         types <- coldata[notOK, 2]
#         tdata <- sqlTypeInfo(channel)
#         coldata[notOK, 3L] <- tdata[match(types, tdata[,
#                                                        2L]), 3L]
#       }
#       if (odbcUpdate(channel, query, mydata, coldata[m, ],
#                      test = test, verbose = verbose, nastring = nastring) <
#           0)
#         return(-1L)
#     }
#     return(invisible(1L))
#   }
#
#   environment(custom_sqlwrite) <- asNamespace('RODBC')
#   assignInNamespace("sqlwrite", custom_sqlwrite, ns = "RODBC")
#
# }


# add_data_to_table_on_server <- function(data, table_name, interface_column_names = TRUE) {
#
#   modify_rodbc_to_work_with_temporal()
#
#   #Can we change this if statement to interface_column_names == FALSE? The code
#   #as written works fine, obvi, but takes a few seconds to understand.
#   if (!interface_column_names) { #if engine column names used, then do clean-up first
#     data <-
#       data %>%
#       #In the line below, is there a reason to say !interface_column_names
#       #rather than FALSE?
#       clean_up_data_types(engine_column_names = !interface_column_names) %>%
#       rename_with(.fn = get_interface_column_names)
#   }
#
#   #connect
#   connection <- connect_to_server()
#
#
#   #check if table exists:
#   if (!(table_name %in% sqlTables(connection)$TABLE_NAME)) {
#     stop ('This table does not exist on the SQL server.')
#   }
#
#   #prepend schema
#   full_table_name <- str_c(get_server_details()$Schema, '.', table_name)
#
#   #add the data
#   sqlSave(connection, data, full_table_name, append = TRUE, rownames = FALSE, verbose = TRUE)
#
#   #close connection
#   odbcClose(connection)
#
# }


#' Change Data Frame Column Names Using a Rosetta Stone
#'
#' This function changes the column names of a data frame based on a mapping provided in a "rosetta" table. It uses a custom function to fill missing values in the new column names with the original names if necessary. Optionally, it can drop the column used for filling in the missing names.
#'
#' @param df A data frame whose column names need to be changed.
#' @param rosetta A data frame containing at least two columns: one for the current names and another for the new names. The names of these columns are specified by `from` and `to`.
#' @param from A string specifying the name of the column in `rosetta` that contains the current names. Defaults to "from".
#' @param to A string specifying the name of the column in `rosetta` that contains the new names. Defaults to "to".
#'
#' @return A data frame with the column names changed according to the mapping provided in `rosetta`.
#'
#' @examples
#' \dontrun{
#' df <- data.frame(A = 1:3, B = 4:6, C = 7:9)
#' rosetta <- data.frame(from = c("A", "B"), to = c("X", "Y"))
#' ezql_change_names(df, rosetta, from = "from", to = "to")
#' }
#'
#' @export
ezql_change_names <- function(df, rosetta, from = "from", to = "to") {
  if (min(c(from, to) %in% names(rosetta)) != 1){
    stop("One or more of from and/or to are not names in rosetta.")
  }
  rosetta <- rosetta %>%
    dplyr::select(dplyr::all_of(c(from, to))) %>%
    rlang::set_names(c("current", "to"))

  new_names <- tibble::tibble(current = names(df)) %>%
    dplyr::left_join(rosetta) %>%
    gplyr::merge_if_na(to, current) %>%
    dplyr::pull(to)

  df %>%
    rlang::set_names(new_names)
}


# get_interface_column_names <- function(column_names, underscores = TRUE) {
#
#   column_names <-
#     data.frame(engine_name = column_names) %>%
#     left_join(column_names_and_types_global) %>%
#     pull(interface_name)
#
#   if (any(is.na(column_names))) {
#     print("Warning! One or more input engine names are not defined in column_names_and_types.csv file!")
#
#     na_indices <- which(is.na(column_names))
#     column_names[na_indices] <- str_c('Unknown', na_indices)
#   }
#
#   if (!underscores) {
#     column_names <-
#       column_names %>%
#       str_replace_all(., '_', ' ')
#   }
#
#   return(column_names)
# }
#
# get_engine_column_names <- function(column_names) {
#
#   column_names <-
#     data.frame(interface_name = column_names) %>%
#     left_join(column_names_and_types_global) %>%
#     pull(engine_name)
#
#   if (any(is.na(column_names))) {
#     print("Warning! One or more input interface names are not defined in column_names_and_types.csv file!")
#
#     na_indices <- which(is.na(column_names))
#     column_names[na_indices] <- str_c('Unknown', na_indices)
#   }
#
#   return(column_names)
# }



#' Set Data Types for a DataFrame Based on a Rosetta Mapping
#'
#' This function applies type transformations to columns in a dataframe based on a provided mapping (rosetta). The function checks for the presence of the specified columns and functions, applies the transformations, and then restores any previous grouping on the dataframe.
#'
#' @param df A dataframe whose columns are to be transformed.
#' @param rosetta A dataframe containing at least two columns: one with the names of the columns to transform (`names_column`) and another with the names of the functions to apply (`type_function_column`).
#' @param names_column A string specifying the name of the column in `rosetta` that contains the column names of `df`. Defaults to "names".
#' @param type_function_column A string specifying the name of the column in `rosetta` that contains the function names as strings. Defaults to "type_function".
#'
#' @return A dataframe with the specified columns transformed according to the functions in `rosetta`. The dataframe will have its original grouping restored if it was grouped.
#'
#' @examples
#' \dontrun{
#' df <- tibble::tibble(A = c("1", "2", "3"), B = c("TRUE", "FALSE", "TRUE"))
#' rosetta <- tibble::tibble(names = c("A", "B"), type_function = c("as.numeric", "as.logical"))
#' df <- ezql_set_data_types(df, rosetta)
#' }
#'
#' @import dplyr
#' @import rlang
#' @export
ezql_set_data_types <- function(df, rosetta, names_column = "names", type_function_column = "type_function") {

  rosetta_column_names <- rosetta %>% dplyr::pull(names_column)
  rosetta_functions <- rosetta %>% dplyr::pull(type_function_column)

  if(!(names_column %in% names(rosetta))) {
    stop(stringr::str_c("A column called '", names_column, "' is not present in the rosetta data frame."))
  }

  if(!(type_function_column %in% names(rosetta))) {
    stop(stringr::str_c("A column called '", type_function_column, "' is not present in the rosetta data frame."))
  }

  if (!all(rosetta_column_names %in% names(df))) {
    warning("Some column names in the rosetta are not present in the df. These columns will not be modified.")
  }

  if(!all(names(df) %in% rosetta_column_names)){
    warning("Not all variables in df are listed in the rosetta. These variables will remain unchanged.")
  }

  grouping_vars <- dplyr::group_vars(df)

  apply_formula <- function (df, variable, fun) {

    if (!exists(fun, mode = "function")) {
      stop(stringr::str_c("The function '", fun, "' does not exist."))
    }

    df %>%
      dplyr::mutate(dplyr::across(dplyr::any_of(variable), get(fun)))
  }

  df <- df %>%
    dplyr::ungroup() %>%
    listful::build2(rosetta_column_names,
                    rosetta_functions,
                    apply_formula)

  if (length(grouping_vars) > 0) {
    df <- df %>% dplyr::group_by(!!!rlang::syms(grouping_vars))
  }

  return(df)

}





#' Execute SQL Queries on a Server
#'
#' This function executes one or more SQL queries on a specified SQL server and returns the results or error messages.
#'
#' @param queries A character vector of SQL queries to execute.
#' @param database A string specifying the database name. Defaults to `NULL` and uses the value from `ezql_details_db()` if not provided.
#' @param address A string specifying the SQL server address. Defaults to `NULL` and uses the value from `ezql_details_add()` if not provided.
#' @return A list containing the results of each query, or error messages if any query fails.
#' @importFrom RODBC sqlQuery odbcClose
#' @importFrom purrr map
#' @export
ezql_query <- function(queries,
                       database = NULL,
                       address = NULL) {

  # Retrieve default values if not provided
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null(database) || is.null(address)) {
    stop("Database, and address must be provided either as arguments or automatically through ezql_details.")
  }

  # Establish the SQL connection
  connection <- ezql_connect(database, address)

  # Ensure connection is closed after all operations
  on.exit(RODBC::odbcClose(connection))

  # Define a helper function to execute a single query and return the result or an error message
  execute_query <- function(connection, query) {
    result <- RODBC::sqlQuery(connection, query)
    if (inherits(result, "try-error")) {
      return(list(success = FALSE, error = result))
    } else {
      return(list(success = TRUE, result = result))
    }
  }

  # Run each query sequentially and collect results using purrr::map
  results <- purrr::map(queries, execute_query, connection = connection)

  return(results)  # Return the list of results
}




#' Assign Primary Key to a SQL Table if None Exists
#'
#' This function assigns a primary key to a specified SQL table if it doesn't already have one.
#'
#' @param table A string specifying the name of the SQL table to modify.
#' @param primary_key_column A string specifying the name of the column to set as the primary key.
#' @param schema A string specifying the schema. Defaults to `NULL` and uses the value from `ezql_details_schema()` if not provided.
#' @param database A string specifying the database name. Defaults to `NULL` and uses the value from `ezql_details_db()` if not provided.
#' @param address A string specifying the SQL server address. Defaults to `NULL` and uses the value from `ezql_details_add()` if not provided.
#' @return A list containing the result of the primary key assignment query, or a warning if the primary key already exists.
#' @importFrom stringr str_c
#' @importFrom RODBC sqlQuery odbcClose
#' @export
ezql_assign_primary_key <- function(table,
                                    primary_key_column,
                                    schema = NULL,
                                    database = NULL,
                                    address = NULL) {

  # Retrieve default values if not provided
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null (schema) || is.null(database) || is.null(address)) {
    stop("Shema, database, and address must be provided either as arguments or automatically through ezql_details.")
  }

  # Construct full table name
  full_table_name <- ezql_full_table_name(schema, table)

  # Query to check if primary key already exists
  check_pk_query <- stringr::str_c(
    "SELECT COUNT(*) AS pk_exists FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS ",
    "WHERE TABLE_SCHEMA = '", schema, "' AND TABLE_NAME = '", table, "' ",
    "AND CONSTRAINT_TYPE = 'PRIMARY KEY';"
  )

  # Execute the query to check for an existing primary key
  pk_check_result <- ezql_query(check_pk_query, database, address)

  # Check the result of the primary key check query
  if (!pk_check_result[[1]]$success) {
    stop("Failed to check for existing primary key: ", pk_check_result[[1]]$error)
  }

  # If no primary key exists, add one; otherwise, give a warning
  if (pk_check_result[[1]]$result$pk_exists == 0) {
    # Create the query to add the primary key
    primary_key_query <- stringr::str_c(
      'ALTER TABLE ', full_table_name,
      ' ADD CONSTRAINT PK_', table,
      ' PRIMARY KEY (', primary_key_column, ');'
    )

    # Execute the query to add the primary key
    pk_add_result <- ezql_query(primary_key_query, database, address)

    # Check the result of the primary key addition query
    if (!pk_add_result[[1]]$success) {
      stop("Failed to add primary key: ", pk_add_result[[1]]$error)
    } else {
      return(pk_add_result[[1]]$result)  # Return the result of the successful query
    }
  } else {
    warning(
      sprintf(
        "Primary key already exists for table '%s.%s'. No primary key was added.",
        schema, table
      )
    )
    return(pk_check_result[[1]]$result)  # Return the result indicating the primary key exists
  }
}


#' Make a Table Temporal in SQL Server
#'
#' This function converts a specified SQL table into a system-versioned temporal table.
#'
#' @param table A string specifying the name of the SQL table to convert to temporal.
#' @param schema A string specifying the schema. Defaults to `NULL` and uses the value from `ezql_details_schema()` if not provided.
#' @param database A string specifying the database name. Defaults to `NULL` and uses the value from `ezql_details_db()` if not provided.
#' @param address A string specifying the SQL server address. Defaults to `NULL` and uses the value from `ezql_details_add()` if not provided.
#' @param primary_key_column A string specifying the name of the column to set as the primary key. Defaults to "ID".
#' @return No return value. The function executes SQL queries to alter the table.
#' @importFrom stringr str_c
#' @export
ezql_make_temporal <- function(table,
                               primary_key_column,
                               schema = NULL,
                               database = NULL,
                               address = NULL
) {

  # Retrieve default values if not provided
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null (schema) || is.null(database) || is.null(address)) {
    stop("Shema, database, and address must be provided either as arguments or automatically through ezql_details.")
  }


  # Assign primary key if necessary
  ezql_assign_primary_key(table, primary_key_column, schema, database, address)

  # Construct full table names and constraints
  full_table_name <- ezql_full_table_name(schema, table)
  from_constraint <- stringr::str_c('DF_', table, '_ValidFrom')
  to_constraint <- stringr::str_c('DF_', table, '_ValidTo')
  history_table_name <- stringr::str_c(full_table_name, '_history')

  # Create the queries with corrected SQL syntax
  query1 <- stringr::str_c(
    'ALTER TABLE ', full_table_name, ' ADD ',
    'ValidFrom datetime2 (2) GENERATED ALWAYS AS ROW START HIDDEN constraint ', from_constraint, ' DEFAULT DATEADD(second, -1, SYSDATETIME()), ',
    'ValidTo datetime2 (2) GENERATED ALWAYS AS ROW END HIDDEN constraint ', to_constraint, " DEFAULT '9999.12.31 23:59:59.99', ",
    'PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo);'
  )

  query2 <- stringr::str_c(
    'ALTER TABLE ', full_table_name, ' SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = ', history_table_name, '));'
  )
  print (c(query1, query2))
  # Execute both queries sequentially using the modified ezql_query
  ezql_query(c(query1, query2), database, address)
}

#' Check if a Table Exists in the Database
#'
#' This function checks if a specified table exists in the given schema within the database.
#'
#' @param table A string specifying the name of the table to check for existence.
#' @param schema A string specifying the schema name. Defaults to `NULL` and uses the value from `ezql_details_schema()` if not provided.
#' @param database A string specifying the database name. Defaults to `NULL` and uses the value from `ezql_details_db()` if not provided.
#' @param address A string specifying the SQL server address. Defaults to `NULL` and uses the value from `ezql_details_add()` if not provided.
#' @return Logical `TRUE` if the table exists, `FALSE` otherwise.
#' @details This function checks the existence of a table by querying the `INFORMATION_SCHEMA.TABLES` view in the specified schema and database.
#' @importFrom stringr str_c
#' @export
ezql_table_exists <- function(table, schema = NULL, database = NULL, address = NULL) {
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided either as arguments or automatically through ezql_details.")
  }

  # Construct SQL query to check table existence
  query <- stringr::str_c(
    "SELECT COUNT(*) AS TableCount FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '",
    schema, "' AND TABLE_NAME = '", table, "';"
  )

  # Execute the query using ezql_query
  result <- ezql_query(query, database, address)

  if (!result[[1]]$success) {
    stop("Error checking table existence: ", result[[1]]$error)
  }

  return(result[[1]]$result$TableCount > 0)
}

#' Assign Primary Key to a SQL Table if None Exists
#'
#' This function assigns a primary key to a specified SQL table if it doesn't already have one.
#'
#' @param table A string specifying the name of the SQL table to modify.
#' @param primary_key_column A string specifying the name of the column to set as the primary key.
#' @param schema A string specifying the schema. Defaults to `NULL` and uses the value from `ezql_details_schema()` if not provided.
#' @param database A string specifying the database name. Defaults to `NULL` and uses the value from `ezql_details_db()` if not provided.
#' @param address A string specifying the SQL server address. Defaults to `NULL` and uses the value from `ezql_details_add()` if not provided.
#' @return A list containing the result of the primary key assignment query, or a warning if the primary key already exists.
#' @importFrom stringr str_c
#' @importFrom RODBC sqlQuery odbcClose
#' @export
ezql_assign_primary_key <- function(table,
                                    primary_key_column,
                                    schema = NULL,
                                    database = NULL,
                                    address = NULL) {

  # Retrieve default values if not provided
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null (schema) || is.null(database) || is.null(address)) {
    stop("Shema, database, and address must be provided either as arguments or automatically through ezql_details.")
  }

  # Construct full table name
  full_table_name <- ezql_full_table_name(schema, table)

  # Query to check if primary key already exists
  check_pk_query <- stringr::str_c(
    "SELECT COUNT(*) AS pk_exists FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS ",
    "WHERE TABLE_SCHEMA = '", schema, "' AND TABLE_NAME = '", table, "' ",
    "AND CONSTRAINT_TYPE = 'PRIMARY KEY';"
  )

  # Execute the query to check for an existing primary key
  pk_check_result <- ezql_query(check_pk_query, database, address)

  # Check the result of the primary key check query
  if (!pk_check_result[[1]]$success) {
    stop("Failed to check for existing primary key: ", pk_check_result[[1]]$error)
  }

  # If no primary key exists, add one; otherwise, give a warning
  if (pk_check_result[[1]]$result$pk_exists == 0) {
    # Create the query to add the primary key
    primary_key_query <- stringr::str_c(
      'ALTER TABLE ', full_table_name,
      ' ADD CONSTRAINT PK_', table,
      ' PRIMARY KEY (', primary_key_column, ');'
    )

    # Execute the query to add the primary key
    pk_add_result <- ezql_query(primary_key_query, database, address)

    # Check the result of the primary key addition query
    if (!pk_add_result[[1]]$success) {
      stop("Failed to add primary key: ", pk_add_result[[1]]$error)
    } else {
      return(pk_add_result[[1]]$result)  # Return the result of the successful query
    }
  } else {
    warning(
      sprintf(
        "Primary key already exists for table '%s.%s'. No primary key was added.",
        schema, table
      )
    )
    return(pk_check_result[[1]]$result)  # Return the result indicating the primary key exists
  }
}


#' Internal Function to Create a New Table in the Database
#'
#' This internal function creates a new table in the specified schema and database using the provided data frame and a rosetta data frame that maps R column names to SQL column names and types.
#'
#' @param df A data frame containing the data to be inserted into the new table.
#' @param table A string specifying the name of the table to be created.
#' @param rosetta A data frame that maps R column names to SQL column names and SQL types. This mapping ensures the correct data types are used in the SQL table.
#' @param rosetta_names_col A string specifying the column name in the rosetta data frame that contains the SQL column names. Defaults to `"sql_name"`.
#' @param rosetta_sql_types_col A string specifying the column name in the rosetta data frame that contains the SQL data types. Defaults to `"sql_type"`.
#' @param schema A string specifying the schema name. Defaults to `NULL` and uses the value from `ezql_details_schema()` if not provided.
#' @param database A string specifying the database name. Defaults to `NULL` and uses the value from `ezql_details_db()` if not provided.
#' @param address A string specifying the SQL server address. Defaults to `NULL` and uses the value from `ezql_details_add()` if not provided.
#' @param primary_key A string specifying the column to set as the primary key. Defaults to `NULL` and no primary key is set if not provided.
#' @return No return value. This function is called for its side effects of creating a table in the specified schema and database.
#' @details This function connects to the specified SQL server, creates a new table according to the schema provided in the rosetta data frame, and inserts the data from the provided data frame. The function will stop and throw an error if there are any issues with the rosetta mapping, such as duplicate column names or mismatches with the data frame.
#' @importFrom stringr str_c
#' @importFrom rlang sym
#' @importFrom RODBC sqlSave odbcClose
#' @importFrom dplyr left_join
#' @importFrom tibble tibble
#' @keywords internal
ezql_new_internal <- function(df,
                              table,
                              rosetta,
                              rosetta_names_col = "sql_name",
                              rosetta_sql_types_col = "sql_type",
                              schema = NULL,
                              database = NULL,
                              address = NULL,
                              primary_key = NULL) {

  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided either as arguments or automatically through ezql_details.")
  }

  rosetta_names <- rosetta[[rosetta_names_col]]

  if(!identical(unique(rosetta_names), rosetta_names)){
    stop(stringr::str_c("The rosetta contains duplicate values in the ", rosetta_names_col, " column."))
  }

  if(!all (names(df) %in% rosetta_names)){
    stop("Every column in df must be included in rosetta.")
  }

  rosetta_sql_types <- rosetta [[rosetta_sql_types_col]]
  names(rosetta_sql_types) <- rosetta[[rosetta_names_col]]

  full_table_name <- ezql_full_table_name(schema, table)

  print(full_table_name)

  # Connect to server
  connection <- ezql_connect(database, address)

  # Create the table
  RODBC::sqlSave(connection, df, full_table_name, rownames = FALSE, varTypes = rosetta_sql_types)

  # Close connection after table creation
  RODBC::odbcClose(connection)

  # Set the primary key if specified using ezql_assign_primary_key
  if (!is.null(primary_key)) {
    ezql_assign_primary_key(table, primary_key, schema = schema, database = database, address = address)
  }
}



#' Create a New Table in the Database if it Does Not Exist
#'
#' This function creates a new table in the specified schema and database using the provided data frame if the table does not already exist.
#'
#' @param df A data frame containing the data to be inserted into the new table.
#' @param table A string specifying the name of the table to be created.
#' @param rosetta A data frame that maps R column names to SQL column names and types.
#' @param rosetta_names_col A string specifying the column name in the rosetta data frame that contains the SQL column names. Defaults to `"sql_name"`.
#' @param rosetta_sql_types_col A string specifying the column name in the rosetta data frame that contains the SQL data types. Defaults to `"sql_type"`.
#' @param schema A string specifying the schema name. Defaults to `NULL` and uses the value from `ezql_details_schema()` if not provided.
#' @param database A string specifying the database name. Defaults to `NULL` and uses the value from `ezql_details_db()` if not provided.
#' @param address A string specifying the SQL server address. Defaults to `NULL` and uses the value from `ezql_details_add()` if not provided.
#' @param primary_key A string specifying the column to set as the primary key. Defaults to `NULL` and no primary key is set if not provided.
#' @return No return value. Stops with an error message if the table already exists.
#' @details This function checks for the existence of the specified table and only creates it if it does not exist, using the `ezql_new_internal` function for the actual creation process.
#' @importFrom stringr str_c
#' @export
ezql_new <- function(df,
                     table,
                     rosetta,
                     rosetta_names_col = "sql_name",
                     rosetta_sql_types_col = "sql_type",
                     primary_key = NULL,
                     schema = NULL,
                     database = NULL,
                     address = NULL) {
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided either as arguments or automatically through ezql_details.")
  }

  if (ezql_table_exists(table, schema, database, address)) {
    stop(stringr::str_c("Table '", schema, ".", table, "' already exists."))
  }

  ezql_new_internal(df, table, rosetta,
                    rosetta_names_col = rosetta_names_col,
                    rosetta_sql_types_col = rosetta_sql_types_col,
                    schema = schema, database = database, address = address,
                    primary_key = primary_key)
}

#' Replace an Existing Table in the Database
#'
#' This function replaces an existing table in the specified schema and database with a new one created from the provided data frame. It stops with an error if the table does not exist.
#'
#' @param df A data frame containing the data to be inserted into the new table.
#' @param table A string specifying the name of the table to be replaced.
#' @param rosetta A data frame that maps R column names to SQL column names and types.
#' @param rosetta_names_col A string specifying the column name in the rosetta data frame that contains the SQL column names. Defaults to `"sql_name"`.
#' @param rosetta_sql_types_col A string specifying the column name in the rosetta data frame that contains the SQL data types. Defaults to `"sql_type"`.
#' @param schema A string specifying the schema name. Defaults to `NULL` and uses the value from `ezql_details_schema()` if not provided.
#' @param database A string specifying the database name. Defaults to `NULL` and uses the value from `ezql_details_db()` if not provided.
#' @param address A string specifying the SQL server address. Defaults to `NULL` and uses the value from `ezql_details_add()` if not provided.
#' @param primary_key A string specifying the column to set as the primary key. Defaults to `NULL` and no primary key is set if not provided.
#' @return No return value. Stops with an error message if the table does not exist.
#' @details This function checks for the existence of the specified table, drops it if it exists, and creates a new table using the `ezql_new_internal` function.
#' @importFrom stringr str_c
#' @export
ezql_replace <- function(df,
                         table,
                         rosetta,
                         rosetta_names_col = "sql_name",
                         rosetta_sql_types_col = "sql_type",
                         primary_key = NULL,
                         schema = NULL,
                         database = NULL,
                         address = NULL) {
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided either as arguments or automatically through ezql_details.")
  }

  if (!ezql_table_exists(table, schema, database, address)) {
    stop(stringr::str_c("Table '",
                        ezql_full_table_name(schema, table),
                        "' does not exist."))
  }

  # Drop the existing table using ezql_query
  drop_query <- stringr::str_c("DROP TABLE ", ezql_full_table_name(schema, table), ";")
  drop_result <- ezql_query(drop_query, database, address)

  if (!drop_result[[1]]$success) {
    stop("Error dropping table: ", drop_result[[1]]$error)
  }

  ezql_new_internal(df, table, rosetta,
                    rosetta_names_col = rosetta_names_col,
                    rosetta_sql_types_col = rosetta_sql_types_col,
                    schema = schema, database = database, address = address,
                    primary_key = primary_key)
}

#' Construct Full Table Name for SQL Queries
#'
#' This function constructs the full table name, including the schema, for use in SQL queries.
#'
#' @param schema A string specifying the schema name.
#' @param table A string specifying the table name.
#' @return A string representing the full table name in the format `schema.table`, which is suitable for use in SQL queries.
#' @details The function combines the schema and table names with a period (.) to create a fully qualified table name that can be used in SQL queries. This is useful for ensuring that the correct table is referenced, especially in databases with multiple schemas.
#' @importFrom stringr str_c
#' @export
ezql_full_table_name <- function(schema, table) {
  stringr::str_c(schema, ".", table)
}
