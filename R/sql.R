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

#' Retrieve Column Names of a SQL Table
#'
#' This function retrieves the column names of a specified table from a SQL database.
#'
#' @param table A string specifying the name of the SQL table.
#' @param schema A string specifying the schema of the SQL table. Defaults to the value
#' retrieved from \code{ezql_details_schema()}.
#' @param database A string specifying the database name. Defaults to the value
#' retrieved from \code{ezql_details_db()}.
#' @param address A string specifying the server address. Defaults to the value
#' retrieved from \code{ezql_details_add()}.
#'
#' @return A character vector of column names in the specified table.
#'
#' @details
#' The function constructs and executes a SQL query to fetch the column names of the specified
#' table. If \code{schema}, \code{database}, or \code{address} are not explicitly provided,
#' they are inferred from predefined global defaults.
#'
#' @examples
#' \dontrun{
#' # Retrieve column names from a table
#' column_names <- ezql_table_names(
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server"
#' )
#' print(column_names)
#' }
#'
#' @seealso \code{\link{ezql_query}}, \code{\link{ezql_connect}}
#' @export
ezql_table_names <- function(table, schema = NULL, database = NULL, address = NULL) {
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided either as arguments or automatically through ezql_details.")
  }

  # Construct SQL query to retrieve column names
  query <- stringr::str_c(
    "SELECT TOP 0 * FROM ", schema, ".", table
  )

  # Execute the query and extract column names
  result <- ezql_query(query, database, address)
  column_names <- names(result[[1]]$result)

  return(column_names)
}


#' Add Data to a SQL Table with Data Type Handling
#'
#' Appends data from a data frame to a specified table in a SQL database, with optional data type transformations based on a "rosetta" data frame.
#' Ensures that the column names in the data frame match those in the table and applies type conversions if specified.
#'
#' @param df A data frame containing the data to add.
#' @param table A string specifying the name of the target SQL table.
#' @param breakdown A tibble returned by \code{ezql_check_table()} with pre-validated data.
#' Defaults to \code{NULL}, and validation is performed within the function.
#' @param schema A string specifying the schema of the table. Defaults to the value
#' retrieved from \code{ezql_details_schema()}.
#' @param database A string specifying the database name. Defaults to the value
#' retrieved from \code{ezql_details_db()}.
#' @param address A string specifying the server address. Defaults to the value
#' retrieved from \code{ezql_details_add()}.
#' @param rosetta A data frame (or tibble) used for column-level data type transformations.
#' This should contain at least two columns: one for column names and one for the associated type transformation functions.
#' Defaults to \code{NULL}, meaning no transformations will be applied.
#' @param names_column A string specifying the column in the \code{rosetta} data frame
#' that contains the names of the columns to be transformed. Defaults to \code{"names"}.
#' @param type_function_column A string specifying the column in the \code{rosetta} data frame
#' that contains the type transformation functions for the corresponding columns. Defaults to \code{"type_function"}.
#'
#' @return No return value. This function is called for its side effects of adding
#' data to the specified SQL table. If no rows are added, a message is printed.
#'
#' @details
#' If \code{breakdown} is provided, the function assumes that the data frame has
#' already been validated. Otherwise, it runs \code{ezql_check_table()} to validate
#' the input. If \code{rosetta} is provided, it applies the specified type transformations
#' to the columns in the data frame before adding the data to the SQL table. Columns
#' not listed in the \code{rosetta} will remain unchanged.
#'
#' The \code{rosetta} data frame must have one column (specified by \code{names_column})
#' listing the column names in \code{df} to be transformed, and another column
#' (specified by \code{type_function_column}) containing the names of functions
#' to be applied for the transformations.
#'
#' @examples
#' \dontrun{
#' # Add rows to a table without transformations
#' ezql_add_data(
#'   df = my_data,
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server"
#' )
#'
#' # Add rows with type transformations using a rosetta data frame
#' rosetta <- tibble::tibble(
#'   names = c("column1", "column2"),
#'   type_function = c("as.character", "as.numeric")
#' )
#'
#' ezql_add_data(
#'   df = my_data,
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server",
#'   rosetta = rosetta
#' )
#' }
#'
#' @seealso \code{\link{ezql_check_table}}
#' @export
ezql_add_data <- function(df,
                          table,
                          breakdown = NULL,
                          schema = NULL,
                          database = NULL,
                          address = NULL,
                          rosetta = NULL,
                          names_column = "names",
                          type_function_column = "type_function"
                          ) {

  # Perform checks and get breakdown
  if(is.null(breakdown)) {
    breakdown <- breakdown <-
      ezql_check_table(df, table, schema = schema, database = database, address,
                       rosetta, names_column, type_function_column)
  }

  schema <- breakdown$schema
  database <- breakdown$database
  address <- breakdown$address
  data_added <- breakdown$data_added[[1]]

  # Exit early if there are no rows to add
  if (nrow(data_added) == 0) {
    message("No rows to add.")
    return(invisible(NULL))
  }

  # Connect to the SQL server
  connection <- ezql_connect(database, address)
  full_table_name <- ezql_full_table_name(schema, table)

  # Add the data
  tryCatch(
    {
      custom_sqlSave(
        channel = connection,
        dat = data_added,
        tablename = full_table_name,
        append = TRUE,
        rownames = FALSE,
        verbose = TRUE
      )
      message("Rows successfully added.")
    },
    error = function(e) {
      stop("Failed to add rows: ", e$message)
    }
  )

  # Close the connection
  RODBC::odbcClose(connection)
}


#' Update Data in a SQL Table with Data Type Handling
#'
#' Updates rows in a specified SQL table using a data frame, with optional data type transformations based on a "rosetta" data frame. Ensures the data frame matches the table structure and updates only rows that differ.
#'
#' @param df A data frame containing the data to update.
#' @param table A string specifying the name of the target SQL table.
#' @param breakdown A tibble returned by \code{ezql_check_table()} with pre-validated data.
#' Defaults to \code{NULL}, and validation is performed within the function.
#' @param schema A string specifying the schema of the table. Defaults to the value
#' retrieved from \code{ezql_details_schema()}.
#' @param database A string specifying the database name. Defaults to the value
#' retrieved from \code{ezql_details_db()}.
#' @param address A string specifying the server address. Defaults to the value
#' retrieved from \code{ezql_details_add()}.
#' @param rosetta A data frame (or tibble) used for column-level data type transformations.
#' This should contain at least two columns: one for column names and one for the associated type transformation functions.
#' Defaults to \code{NULL}, meaning no transformations will be applied.
#' @param names_column A string specifying the column in the \code{rosetta} data frame
#' that contains the names of the columns to be transformed. Defaults to \code{"names"}.
#' @param type_function_column A string specifying the column in the \code{rosetta} data frame
#' that contains the type transformation functions for the corresponding columns. Defaults to \code{"type_function"}.
#'
#' @return No return value. This function is called for its side effects of updating
#' rows in the specified SQL table. If no rows are updated, a message is printed.
#'
#' @details
#' If \code{breakdown} is provided, the function assumes that the data frame has
#' already been validated. Otherwise, it runs \code{ezql_check_table()} to validate
#' the input. If \code{rosetta} is provided, it applies the specified type transformations
#' to the columns in the data frame before attempting the update. Columns not listed
#' in the \code{rosetta} will remain unchanged.
#'
#' The \code{rosetta} data frame must have one column (specified by \code{names_column})
#' listing the column names in \code{df} to be transformed, and another column
#' (specified by \code{type_function_column}) containing the names of functions
#' to be applied for the transformations.
#'
#' @examples
#' \dontrun{
#' # Update rows in a table without transformations
#' ezql_alter_data(
#'   df = updated_data,
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server"
#' )
#'
#' # Update rows with type transformations using a rosetta data frame
#' rosetta <- tibble::tibble(
#'   names = c("column1", "column2"),
#'   type_function = c("as.character", "as.numeric")
#' )
#'
#' ezql_alter_data(
#'   df = updated_data,
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server",
#'   rosetta = rosetta
#' )
#' }
#'
#' @seealso \code{\link{ezql_check_table}}
#' @export
ezql_alter_data <- function(df,
                            table,
                            breakdown = NULL,
                            schema = NULL,
                            database = NULL,
                            address = NULL,
                            rosetta = NULL,
                            names_column = "names",
                            type_function_column = "type_function"
                            ) {

  # Perform checks and get breakdown
  if(is.null(breakdown)) {
    breakdown <-
      ezql_check_table(df, table, schema = schema, database = database, address,
                       rosetta, names_column, type_function_column)
  }
  schema <- breakdown$schema
  database <- breakdown$database
  address <- breakdown$address

  data_altered <- breakdown$data_altered[[1]]

  # Exit early if there are no rows to alter
  if (nrow(data_altered) == 0) {
    message("No rows to update.")
    return(invisible(NULL))
  }

  # Connect to the SQL server
  connection <- ezql_connect(database, address)
  full_table_name <- ezql_full_table_name(schema, table)

  # Perform the update using RODBC::sqlUpdate
  tryCatch(
    {
      RODBC::sqlUpdate(
        channel = connection,
        dat = data_altered,
        tablename = full_table_name,
        index = ezql_primary_key_name(table, schema, database, address),
        verbose = TRUE
      )
      message("Rows successfully updated.")
    },
    error = function(e) {
      stop("Failed to update rows: ", e$message)
    }
  )

  # Close the connection
  RODBC::odbcClose(connection)
}



#' Delete Rows from a SQL Table with Data Type Handling
#'
#' Deletes rows from a SQL table based on a data frame, with optional data type transformations applied to ensure compatibility. Ensures that only rows present in both the data frame and the table are deleted.
#'
#' @param df A data frame containing the rows to delete.
#' @param table A string specifying the name of the target SQL table.
#' @param breakdown A tibble returned by \code{ezql_check_table()} with pre-validated data.
#' Defaults to \code{NULL}, and validation is performed within the function.
#' @param schema A string specifying the schema of the table. Defaults to the value
#' retrieved from \code{ezql_details_schema()}.
#' @param database A string specifying the database name. Defaults to the value
#' retrieved from \code{ezql_details_db()}.
#' @param address A string specifying the server address. Defaults to the value
#' retrieved from \code{ezql_details_add()}.
#' @param rosetta A data frame (or tibble) used for column-level data type transformations.
#' This should contain at least two columns: one for column names and one for the associated type transformation functions.
#' Defaults to \code{NULL}, meaning no transformations will be applied.
#' @param names_column A string specifying the column in the \code{rosetta} data frame
#' that contains the names of the columns to be transformed. Defaults to \code{"names"}.
#' @param type_function_column A string specifying the column in the \code{rosetta} data frame
#' that contains the type transformation functions for the corresponding columns. Defaults to \code{"type_function"}.
#'
#' @return No return value. This function is called for its side effects of deleting
#' rows from the specified SQL table. If no rows are deleted, a message is printed.
#'
#' @details
#' If \code{breakdown} is provided, the function assumes that the data frame has
#' already been validated. Otherwise, it runs \code{ezql_check_table()} to validate
#' the input. If \code{rosetta} is provided, it applies the specified type transformations
#' to the columns in the data frame before attempting the delete operation. Columns
#' not listed in the \code{rosetta} will remain unchanged.
#'
#' The \code{rosetta} data frame must have one column (specified by \code{names_column})
#' listing the column names in \code{df} to be transformed, and another column
#' (specified by \code{type_function_column}) containing the names of functions
#' to be applied for the transformations.
#'
#' Deletes are performed in bulk using the primary key of the table. The function
#' constructs a \code{WHERE} clause based on the primary key values from the data frame.
#'
#' @examples
#' \dontrun{
#' # Delete rows from a table without transformations
#' ezql_delete_data(
#'   df = rows_to_remove,
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server"
#' )
#'
#' # Delete rows with type transformations using a rosetta data frame
#' rosetta <- tibble::tibble(
#'   names = c("column1", "column2"),
#'   type_function = c("as.character", "as.numeric")
#' )
#'
#' ezql_delete_data(
#'   df = rows_to_remove,
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server",
#'   rosetta = rosetta
#' )
#' }
#'
#' @seealso \code{\link{ezql_check_table}}
#' @export
ezql_delete_data <- function(df,
                             table,
                             breakdown = NULL,
                             schema = NULL,
                             database = NULL,
                             address = NULL,
                             rosetta = NULL,
                             names_column = "names",
                             type_function_column = "type_function"
                             ) {
  # Perform checks and get breakdown
  if(is.null(breakdown)) {
    breakdown <-
      ezql_check_table(df, table, schema = schema, database = database, address,
                       rosetta, names_column, type_function_column)
  }
  schema <- breakdown$schema
  database <- breakdown$database
  address <- breakdown$address
  data_to_be_deleted <- breakdown$data_to_be_deleted[[1]]

  # Retrieve primary key
  primary_key_name <- ezql_primary_key_name(table, schema, database, address)

  # Ensure alignment between data_to_be_deleted and df
  rows_to_delete <- data_to_be_deleted %>%
    pull(all_of(primary_key_name))

  # Exit early if no rows to delete
  if (length(rows_to_delete) == 0) {
    message("No rows to delete.")
    return(invisible(NULL))
  }

  # Check the type of the primary key values and construct the WHERE clause accordingly
  if (is.numeric(rows_to_delete)) {
    # Numeric values: no quotes needed
    where_conditions <- str_c(primary_key_name, " IN (", str_c(rows_to_delete, collapse = ", "), ")")
  } else {
    # Character values: wrap in single quotes
    where_conditions <- str_c(primary_key_name, " IN (", str_c(str_c("'", rows_to_delete, "'"), collapse = ", "), ")")
  }

  delete_query <- paste0("DELETE FROM ", schema, ".", table, " WHERE ", where_conditions)

  # Debug: Print query if needed
  # print(delete_query)

  # Connect to the SQL server and execute the bulk delete query
  connection <- ezql_connect(database, address)
  tryCatch(
    {
      res <- RODBC::sqlQuery(connection, delete_query)
      if (!is.null(attr(res, "error"))) {
        stop("Error executing query: ", attr(res, "error"))
      }
      message(length(rows_to_delete), " row(s) deleted from the table.")
    },
    error = function(e) {
      stop("Failed to delete rows: ", e$message)
    }
  )
  RODBC::odbcClose(connection)
}



#' Edit a SQL Table with Data Type Handling
#'
#' A wrapper function that combines adding, updating, and deleting rows in a SQL table
#' based on a data frame. Allows for a comprehensive table update in one step, with
#' optional data type transformations.
#'
#' @param df A data frame containing the new data for the SQL table.
#' @param table A string specifying the name of the target SQL table.
#' @param schema A string specifying the schema of the table. Defaults to the value
#' retrieved from \code{ezql_details_schema()}.
#' @param database A string specifying the database name. Defaults to the value
#' retrieved from \code{ezql_details_db()}.
#' @param address A string specifying the server address. Defaults to the value
#' retrieved from \code{ezql_details_add()}.
#' @param delete_missing_rows Logical. If \code{TRUE}, rows in the SQL table but not
#' in the data frame are deleted. Defaults to \code{FALSE}.
#' @param rosetta A data frame (or tibble) used for column-level data type transformations.
#' This should contain at least two columns: one for column names and one for the associated type transformation functions.
#' Defaults to \code{NULL}, meaning no transformations will be applied.
#' @param names_column A string specifying the column in the \code{rosetta} data frame
#' that contains the names of the columns to be transformed. Defaults to \code{"names"}.
#' @param type_function_column A string specifying the column in the \code{rosetta} data frame
#' that contains the type transformation functions for the corresponding columns. Defaults to \code{"type_function"}.
#'
#' @return A list summarizing the number of rows added, updated, and deleted.
#'
#' @details
#' This function performs a full edit of the specified SQL table by:
#' 1. Adding new rows present in the data frame but not in the SQL table.
#' 2. Updating rows in the SQL table that differ from the corresponding rows in the data frame.
#' 3. Optionally deleting rows in the SQL table that are not present in the data frame (if \code{delete_missing_rows = TRUE}).
#'
#' If \code{rosetta} is provided, the function applies the specified type transformations
#' to the columns in the data frame before attempting any operations. Columns not listed
#' in the \code{rosetta} will remain unchanged.
#'
#' The \code{rosetta} data frame must have one column (specified by \code{names_column})
#' listing the column names in \code{df} to be transformed, and another column
#' (specified by \code{type_function_column}) containing the names of functions
#' to be applied for the transformations.
#'
#' @examples
#' \dontrun{
#' # Edit a table by adding, updating, and optionally deleting rows
#' result <- ezql_edit(
#'   df = new_data,
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server",
#'   delete_missing_rows = TRUE
#' )
#' print(result)
#'
#' # Edit a table with type transformations using a rosetta data frame
#' rosetta <- tibble::tibble(
#'   names = c("column1", "column2"),
#'   type_function = c("as.character", "as.numeric")
#' )
#'
#' result <- ezql_edit(
#'   df = new_data,
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server",
#'   delete_missing_rows = TRUE,
#'   rosetta = rosetta
#' )
#' print(result)
#' }
#'
#' @seealso \code{\link{ezql_add_data}}, \code{\link{ezql_alter_data}}, \code{\link{ezql_delete_data}}, \code{\link{ezql_check_table}}
#' @export
ezql_edit <- function(df,
                      table,
                      schema = NULL,
                      database = NULL,
                      address = NULL,
                      delete_missing_rows = FALSE,
                      rosetta = NULL,
                      names_column = "names",
                      type_function_column = "type_function"
                      ) {
  # Perform checks and get breakdown
  breakdown <-
    ezql_check_table(df, table, schema = schema, database = database, address,
                     rosetta, names_column, type_function_column)

  # Add new rows
  ezql_add_data(df, table, breakdown = breakdown)

  # Update existing rows
  ezql_alter_data(df, table, breakdown = breakdown)

  # Delete rows if specified
  if (delete_missing_rows) {
    ezql_delete_data(df, table, breakdown = breakdown)
  } else {
    warning("Missing rows were not deleted. Set `delete_missing_rows = TRUE` to remove them.")
  }

  # Return a summary
  return(list(
    added = nrow(breakdown$data_added[[1]]),
    altered = nrow(breakdown$data_altered[[1]]),
    deleted = if (delete_missing_rows)
      nrow(breakdown$data_deleted[[1]])
    else
      0
  ))
}


#' Retrieve the Primary Key Name for a SQL Table
#'
#' Fetches the name of the primary key column(s) for a specified table in a SQL database.
#'
#' @param table A string specifying the name of the SQL table.
#' @param schema A string specifying the schema of the table. Defaults to the value
#' retrieved from \code{ezql_details_schema()}.
#' @param database A string specifying the database name. Defaults to the value
#' retrieved from \code{ezql_details_db()}.
#' @param address A string specifying the server address. Defaults to the value
#' retrieved from \code{ezql_details_add()}.
#'
#' @return A character vector of column names that make up the primary key. Returns
#' \code{NULL} if the table has no primary key.
#'
#' @details
#' Queries the SQL server to identify the primary key columns for the specified table.
#' If no primary key exists, the function returns \code{NULL}.
#'
#' @examples
#' \dontrun{
#' # Retrieve the primary key of a table
#' pk <- ezql_primary_key_name(
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server"
#' )
#' print(pk)
#' }
#'
#' @seealso \code{\link{ezql_query}}, \code{\link{ezql_details_schema}}
#' @export
ezql_primary_key_name <- function(table, schema = NULL, database = NULL, address = NULL) {
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided either as arguments or automatically through ezql_details.")
  }

  # SQL query to fetch primary key information
  pk_query <- stringr::str_c(
    "SELECT COLUMN_NAME
     FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
     WHERE TABLE_SCHEMA = '", schema, "' AND TABLE_NAME = '", table, "'"
  )

  # Execute the query using ezql_query
  result <- ezql_query(pk_query, database, address)

  # Extract the result from the returned list
  pk_data <- result[[1]]$result

  # Check if there are any primary keys
  if (nrow(pk_data) == 0) {
    return(NULL)
  }

  # Return the primary key as a tibble
  return((pk_data$COLUMN_NAME))
}


#' Retrieve Primary Key Data from a SQL Table
#'
#' Fetches the data for the primary key column(s) from a specified table in a SQL database.
#'
#' @param table A string specifying the name of the SQL table.
#' @param schema A string specifying the schema of the table. Defaults to the value
#' retrieved from \code{ezql_details_schema()}.
#' @param database A string specifying the database name. Defaults to the value
#' retrieved from \code{ezql_details_db()}.
#' @param address A string specifying the server address. Defaults to the value
#' retrieved from \code{ezql_details_add()}.
#'
#' @return A tibble containing the data for the primary key column(s).
#'
#' @details
#' This function retrieves the primary key column(s) from a specified table
#' and returns their data as a tibble. If the table does not have a primary key,
#' the function stops with an error.
#'
#' @examples
#' \dontrun{
#' # Retrieve primary key data
#' pk_data <- ezql_primary_key_data(
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server"
#' )
#' print(pk_data)
#' }
#'
#' @seealso \code{\link{ezql_primary_key_name}}, \code{\link{ezql_query}}
#' @export
ezql_primary_key_data <- function(table, schema = NULL, database = NULL, address = NULL) {
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided either as arguments or automatically through ezql_details.")
  }

  # Get the primary key name(s)
  primary_key <- ezql_primary_key_name(table, schema, database, address)

  if (is.null(primary_key)) {
    stop("No primary key found for the table. Cannot retrieve primary key data.")
  }

  # Construct a SQL query to retrieve only the primary key column(s)
  pk_query <- stringr::str_c(
    "SELECT ", paste(primary_key, collapse = ", "),
    " FROM ", schema, ".", table
  )

  # Execute the query using ezql_query
  result <- ezql_query(pk_query, database, address)

  # Extract the result from the returned list
  pk_data <- result[[1]]$result

  # Return the primary key data as a tibble
  return(tibble::as_tibble (pk_data))
}


#' Validate a Data Frame Against a SQL Table with Data Type Handling
#'
#' Checks the compatibility of a data frame with a specified SQL table by validating
#' column names, primary keys, and identifying rows to add, update, or delete.
#' Optionally applies data type transformations based on a "rosetta" data frame.
#'
#' @param df A data frame to validate against the SQL table.
#' @param table A string specifying the name of the SQL table.
#' @param schema A string specifying the schema of the table. Defaults to the value
#' retrieved from \code{ezql_details_schema()}.
#' @param database A string specifying the database name. Defaults to the value
#' retrieved from \code{ezql_details_db()}.
#' @param address A string specifying the server address. Defaults to the value
#' retrieved from \code{ezql_details_add()}.
#' @param rosetta A data frame (or tibble) used for column-level data type transformations.
#' This should contain at least two columns: one for column names and one for the associated type transformation functions.
#' Defaults to \code{NULL}, meaning no transformations will be applied.
#' @param names_column A string specifying the column in the \code{rosetta} data frame
#' that contains the names of the columns to be transformed. Defaults to \code{"names"}.
#' @param type_function_column A string specifying the column in the \code{rosetta} data frame
#' that contains the type transformation functions for the corresponding columns. Defaults to \code{"type_function"}.
#'
#' @return A tibble containing:
#' \itemize{
#'   \item \code{data_added}: Rows in \code{df} not present in the table.
#'   \item \code{data_altered}: Rows in \code{df} with matching primary keys but different values.
#'   \item \code{data_deleted}: Rows in the table not present in \code{df}.
#'   \item \code{data_to_be_deleted}: Rows in both \code{df} and the table.
#'   \item \code{schema}: The schema name.
#'   \item \code{database}: The database name.
#'   \item \code{address}: The server address.
#' }
#'
#' @details
#' This function ensures that the data frame and SQL table are compatible for operations
#' like adding, updating, or deleting rows. It performs the following checks:
#' \enumerate{
#'   \item Confirms the SQL table exists on the server.
#'   \item Ensures the table has a primary key.
#'   \item Validates column names in \code{df} against the table structure.
#'   \item Identifies duplicate primary key values in \code{df}.
#'   \item Retrieves existing rows from the SQL table and identifies differences.
#' }
#'
#' If \code{rosetta} is provided, the function applies type transformations to the server data
#' before performing any comparisons. This ensures compatibility between the data frame and
#' the SQL table when column types differ.
#'
#' The \code{rosetta} data frame must have one column (specified by \code{names_column})
#' listing the column names in \code{df} to be transformed, and another column
#' (specified by \code{type_function_column}) containing the names of functions
#' to be applied for the transformations.
#'
#' @examples
#' \dontrun{
#' # Validate a data frame against a table
#' breakdown <- ezql_check_table(
#'   df = my_data,
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server"
#' )
#' print(breakdown$data_added)
#'
#' # Validate with type transformations using a rosetta data frame
#' rosetta <- tibble::tibble(
#'   names = c("column1", "column2"),
#'   type_function = c("as.character", "as.numeric")
#' )
#' breakdown <- ezql_check_table(
#'   df = my_data,
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server",
#'   rosetta = rosetta
#' )
#' print(breakdown$data_added)
#' }
#'
#' @seealso \code{\link{ezql_primary_key_name}}, \code{\link{ezql_table_names}}, \code{\link{ezql_get}}, \code{\link{ezql_set_data_types}}
#' @export
ezql_check_table <- function(df, table, schema = NULL, database = NULL, address = NULL,
                             rosetta = NULL, names_column = "names",
                             type_function_column = "type_function"
                             ) {
  # Resolve defaults
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided either as arguments or automatically through ezql_details.")
  }

  # Step 1: Confirm that the table exists
  if (!ezql_table_exists(table, schema, database, address)) {
    stop("This table does not exist on the SQL server.")
  }

  # Step 2: Retrieve primary key and confirm its presence
  primary_key <- ezql_primary_key_name(table, schema, database, address)
  if (is.null(primary_key)) {
    stop("The table does not have a primary key.")
  }

  # Step 3: Retrieve server table column names and confirm column name matching
  server_table_names <- ezql_table_names(table, schema, database, address)
  if (!identical(sort(names(df)), sort(server_table_names))) {
    stop("The dataframe provided and the table on the server have different column names.")
  }

  # Step 4: Check for duplicate primary keys in the dataframe
  duplicate_keys <- df %>%
    dplyr::group_by(across(all_of(primary_key))) %>%
    dplyr::summarise(count = dplyr::n(), .groups = "drop") %>%
    dplyr::filter(count > 1)

  if (nrow(duplicate_keys) > 0) {
    stop("The dataframe contains duplicate primary key values:\n",
         paste(capture.output(print(duplicate_keys)), collapse = "\n"))
  }

  # Step 5: Retrieve existing data from the server
  if(is.null(rosetta)){
    server_data <- ezql_get(table, schema = schema, database = database, address = address)
  } else {
    server_data <- ezql_get(table, schema = schema, database = database, address = address) %>%
      ezql_set_data_types(rosetta = rosetta,
                          names_column = names_column,
                          type_function_column = type_function_column)
  }

  # Step 6: Identify rows to add, alter, delete, and to be deleted
  new_and_edited_rows <- dplyr::anti_join(df, server_data)
  data_added <- dplyr::anti_join(new_and_edited_rows, server_data, by = primary_key)
  data_altered <- dplyr::semi_join(new_and_edited_rows, server_data, by = primary_key)
  data_deleted <- dplyr::anti_join(server_data, df, by = primary_key)
  data_to_be_deleted <- dplyr::anti_join(server_data, df, by = primary_key)

  # Return results
  return(tibble::tibble(
    data_added = list(data_added),
    data_altered = list(data_altered),
    data_deleted = list(data_deleted),
    data_to_be_deleted = list(data_to_be_deleted),
    schema = schema,
    database = database,
    address = address
  ))
}


#' Drop a SQL Table
#'
#' Deletes a specified table from a SQL database.
#'
#' @param table A string specifying the name of the SQL table to be dropped.
#' @param schema A string specifying the schema of the table. Defaults to the value
#' retrieved from \code{ezql_details_schema()}.
#' @param database A string specifying the database name. Defaults to the value
#' retrieved from \code{ezql_details_db()}.
#' @param address A string specifying the server address. Defaults to the value
#' retrieved from \code{ezql_details_add()}.
#'
#' @return Invisible \code{NULL}. The function is called for its side effect of
#' deleting the specified table from the database.
#'
#' @details
#' This function constructs and executes a SQL \code{DROP TABLE} query to remove
#' the specified table from the database. If the table does not exist or the query
#' fails, an error is raised.
#'
#' @examples
#' \dontrun{
#' # Drop a table from the database
#' ezql_drop(
#'   table = "my_table",
#'   schema = "dbo",
#'   database = "my_database",
#'   address = "my_server"
#' )
#' }
#'
#' @seealso \code{\link{ezql_query}}, \code{\link{ezql_details_schema}}, \code{\link{ezql_details_db}}
#' @export
ezql_drop <- function(table, schema = NULL, database = NULL, address = NULL) {
  # Retrieve default values if not provided
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided either as arguments or automatically through ezql_details.")
  }

  # Construct full table name
  full_table_name <- ezql_full_table_name(schema, table)

  # Construct the SQL query to delete the table
  delete_query <- stringr::str_c("DROP TABLE ", full_table_name, ";")

  # Execute the query using ezql_query
  result <- ezql_query(delete_query, database, address)

  # Check the result and handle errors or success
  if (!result[[1]]$success) {
    stop("Failed to delete the table: ", result[[1]]$error)
  } else {
    message("Table '", full_table_name, "' was successfully deleted.")
    return(invisible(NULL))
  }
}


