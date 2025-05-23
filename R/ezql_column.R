
#' Add a Column to a SQL Table
#'
#' Adds a new column to a specified SQL Server table using T-SQL ALTER TABLE.
#'
#' @param table A string specifying the name of the table to modify.
#' @param column_name A string specifying the name of the new column to add.
#' @param data_type A string specifying the SQL Server data type (e.g., 'VARCHAR(255)', 'INT', 'DATETIME2').
#' @param null Logical indicating whether the column should allow NULLs. Default is TRUE.
#' @param default A value to set as the default for the column (optional). This will be automatically quoted if it's a character string and not already SQL code (e.g., "hi" becomes "'hi'").
#' @param backfill_default Logical. If TRUE, fills existing rows in the table with the default value after adding the column. Default is FALSE.
#' @param schema A string specifying the schema. Defaults to NULL and uses the value from ezql_details_schema().
#' @param database A string specifying the database name. Defaults to NULL and uses the value from ezql_details_db().
#' @param address A string specifying the SQL server address. Defaults to NULL and uses the value from ezql_details_add().
#'
#' @return A list with the result of the query.
#' @export
ezql_column_sql_add <- function(table,
                                column_name,
                                data_type,
                                null = TRUE,
                                default = NULL,
                                backfill_default = FALSE,
                                schema = NULL,
                                database = NULL,
                                address = NULL) {

  if (!isTRUE(null) && is.null(default)) {
    stop("Cannot add a NOT NULL column without a default unless the table is empty.")
  }

  schema   <- schema   %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address  <- address  %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided either as arguments or via ezql_details_*.")
  }

  full_table_name <- ezql_full_table_name(schema, table)

  # Auto-quote character defaults unless already quoted or SQL
  if (!is.null(default) && is.character(default) &&
      !grepl("^'.*'$|\\(.*\\)", default)) {
    default <- paste0("'", default, "'")
  }

  null_clause <- if (stringr::str_detect(stringr::str_to_lower(data_type), "null")) "" else {
    if (isTRUE(null)) "NULL" else "NOT NULL"
  }
  default_clause <- if (!is.null(default)) paste0("DEFAULT ", default) else ""

  query <- stringr::str_c(
    "ALTER TABLE ", full_table_name, " ADD ",
    column_name, " ", data_type, " ", default_clause, " ", null_clause, ";"
  )

  result <- ezql_query(query, database = database, address = address)

  if (!result[[1]]$success) {
    stop("Failed to add column: ", result[[1]]$error)
  }

  # Skip backfill if NOT NULL and default was provided — SQL Server already did it
  auto_default_applied <- stringr::str_detect(stringr::str_to_lower(data_type), "not null") && !is.null(default)

  if (backfill_default && !auto_default_applied) {
    update_query <- stringr::str_c(
      "UPDATE ", full_table_name,
      " SET ", column_name, " = ", default,
      " WHERE ", column_name, " IS NULL;"
    )
    update_result <- ezql_query(update_query, database = database, address = address)

    if (!update_result[[1]]$success) {
      stop("Column added, but failed to backfill existing rows: ", update_result[[1]]$error)
    }
  }


  return(result[[1]]$result)
}


#' Drop a Column from a SQL Table (with automatic constraint removal)
#'
#' Removes a column from a specified SQL Server table. If the column has an associated
#' default constraint (which SQL Server automatically generates when you define a default),
#' this function will first detect and drop the constraint before attempting to drop the column.
#'
#' @param table A string specifying the name of the table to modify.
#' @param column_name A string specifying the name of the column to drop.
#' @param schema A string specifying the schema. Defaults to NULL and uses the value from ezql_details_schema().
#' @param database A string specifying the database name. Defaults to NULL and uses the value from ezql_details_db().
#' @param address A string specifying the SQL server address. Defaults to NULL and uses the value from ezql_details_add().
#'
#' @return A list with the result of the column drop query.
#' @export
ezql_column_sql_drop <- function(table,
                                 column_name,
                                 schema = NULL,
                                 database = NULL,
                                 address = NULL) {

  # Resolve default values if not explicitly provided
  schema <- schema %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address <- address %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided either as arguments or via ezql_details_*.")
  }

  full_table_name <- ezql_full_table_name(schema, table)

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # WHY THIS IS NEEDED:
  # In SQL Server, if a column has a default value defined using DEFAULT,
  # SQL Server *automatically creates a named constraint* for that default.
  #
  # Example:
  #   ALTER TABLE employees ADD created_at DATETIME2 DEFAULT GETDATE();
  #
  # Behind the scenes, SQL Server creates something like:
  #   CONSTRAINT DF__employees__created_at__xxxxx
  #
  # If you later try to DROP the column without removing that constraint,
  # SQL Server will throw an error like:
  #   "The object 'DF__...__...' is dependent on column 'created_at'"
  #
  # This block dynamically looks up the constraint name and removes it
  # before attempting to drop the column.
  # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  drop_constraint_sql <- stringr::str_glue(
    "DECLARE @sql NVARCHAR(MAX);
     SELECT @sql = 'ALTER TABLE {full_table_name} DROP CONSTRAINT [' + dc.name + ']'
     FROM sys.default_constraints dc
     JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
     WHERE OBJECT_NAME(dc.parent_object_id) = '{table}' AND c.name = '{column_name}';
     IF @sql IS NOT NULL EXEC(@sql);"
  )

  constraint_result <- ezql_query(drop_constraint_sql, database = database, address = address)

  if (!constraint_result[[1]]$success) {
    warning("Failed to drop default constraint (if any): ", constraint_result[[1]]$error)
  }

  # Now that the constraint (if it existed) is removed, we can safely drop the column
  drop_column_sql <- stringr::str_c("ALTER TABLE ", full_table_name, " DROP COLUMN ", column_name, ";")
  drop_result <- ezql_query(drop_column_sql, database = database, address = address)

  if (!drop_result[[1]]$success) {
    stop("Failed to drop column: ", drop_result[[1]]$error)
  }

  return(drop_result[[1]]$result)
}

#' Alter (or Rename) a Column in a SQL Table
#' *(data‑type, default, and **rename** – nullability change currently disabled)*
#'
#' This helper lets you:
#' • **rename** a column (old_name → new_name) *via* sp_rename()
#' • change its **data type** (e.g., VARCHAR(50) → VARCHAR(100))
#' • (re)create a **DEFAULT** constraint.
#'
#' **What it does *not* do (yet)**
#' Switching NULL ↔ NOT NULL is still disabled pending a rock‑solid temporal
#' solution (see placeholder note below).
#'
#' *Placeholder note (2025‑05‑13):*  We previously attempted a full
#' nullability routine for system‑versioned tables but hit edge‑cases where
#' SQL Server silently reverted the flag.  Future‑Geordie can revisit once we
#' have a bullet‑proof approach.
#'
#' @param table        Table name.
#' @param column_name  Current column name.
#' @param rename_to    Optional. **New** column name.  NULL → no rename.
#' @param new_type     Optional. New SQL data type, e.g., "VARCHAR(150)".
#' @param default      Optional. Default value **as raw SQL** ('active', 0, GETDATE()).
#'                     NULL → drop existing default without adding a new one.
#' @param schema       Defaults to ezql_details_schema().
#' @param database     Defaults to ezql_details_db().
#' @param address      Defaults to ezql_details_add().
#'
#' @return Invisible NULL (stops on any SQL error).
#' @export


ezql_column_sql_alter <- function(table,
                                  column_name,
                                  rename_to = NULL,
                                  new_type  = NULL,
                                  default   = NULL,
                                  schema    = NULL,
                                  database  = NULL,
                                  address   = NULL) {

  # ---- 0.  Resolve connection defaults ----
  schema   <- schema   %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address  <- address  %||% ezql_details_add()
  if (is.null(schema) || is.null(database) || is.null(address))
    stop("Schema, database, and address must be provided (or set via ezql_details_*).")
  if (is.null(rename_to) && is.null(new_type) && is.null(default))
    stop("Provide at least one of rename_to, new_type or default (or a combo thereof).")

  tbl <- ezql_full_table_name(schema, table)

  # Helper to run SQL and abort on first failure
  run_sql <- function(sql_vec) {
    res <- ezql_query(sql_vec, database = database, address = address)
    for (x in res) if (!x$success) stop("SQL failure: ", x$error)
    invisible(NULL)
  }

  # ---- 1.  Rename column (if requested) ----
  if (!is.null(rename_to) && !identical(rename_to, column_name)) {
    rename_sql <- stringr::str_glue("EXEC sp_rename '{tbl}.{column_name}', '{rename_to}', 'COLUMN';")
    run_sql(rename_sql)
    column_name <- rename_to  # subsequent steps use new name
  }

  # ---- 2.  Drop existing default constraint (always safe) ----
  drop_default <- stringr::str_glue(
    "DECLARE @sql NVARCHAR(MAX);\n",
    "SELECT @sql = 'ALTER TABLE {tbl} DROP CONSTRAINT [' + dc.name + ']'\n",
    "FROM sys.default_constraints dc\n",
    "JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id\n",
    "WHERE OBJECT_NAME(dc.parent_object_id) = '{table}' AND c.name = '{column_name}';\n",
    "IF @sql IS NOT NULL EXEC(@sql);"
  )
  run_sql(drop_default)

  # ---- 3.  ALTER COLUMN data type (if requested) ----
  if (!is.null(new_type)) {
    alter_sql <- stringr::str_glue("ALTER TABLE {tbl} ALTER COLUMN {column_name} {new_type};")
    run_sql(alter_sql)
  }

  # ---- 4.  Add NEW default constraint (if provided) ----
  if (!is.null(default)) {
    default_sql <- if (is.character(default) && !grepl("^'.*'$|\\(.*\\)", default)) paste0("'", default, "'") else default
    constraint  <- stringr::str_c("DF_", table, "_", column_name)
    add_def_sql <- stringr::str_glue("ALTER TABLE {tbl} ADD CONSTRAINT {constraint} DEFAULT {default_sql} FOR {column_name};")
    run_sql(add_def_sql)
  }

  invisible(NULL)
}



#' Retrieve Ezekiel Rosetta Data
#'
#' This function retrieves data from the SQL table named "ezql_rosetta".
#'
#' @return A tibble containing the data from the "ezql_rosetta" SQL table.
#' @export
ezql_rosetta <- function() {
  ezql_get("ezql_rosetta")
}

#' Add a Column Definition to the ezql_rosetta Table
#'
#' Updates the ezql_rosetta table with a new column mapping between R and SQL.
#'
#' @param table A string: the table the column belongs to (not stored, but useful for context).
#' @param r_name A string: the column name in R.
#' @param sql_name A string: the column name in SQL (defaults to camelCase of r_name).
#' @param r_type A string: R type coercion function (e.g., "as.character", "as.numeric").
#' @param sql_type A string: full SQL type declaration (e.g., "nvarchar(50)", "int").
#' @param schema Optional: schema name (defaults to ezql_details_schema()).
#' @param database Optional: database name (defaults to ezql_details_db()).
#' @param address Optional: SQL Server address (defaults to ezql_details_add()).
#'
#' @return A tibble containing the full updated `ezql_rosetta()` table (invisibly). A message also displays a preview of the last 5 rows, including the affected entry.
#' @importFrom utils tail
#' @importFrom glue glue
#' @export
ezql_column_rosetta_add <- function(table,
                                    r_name,
                                    sql_name = NULL,
                                    r_type,
                                    sql_type,
                                    schema = NULL,
                                    database = NULL,
                                    address = NULL) {

  # -- Resolve connection details
  schema   <- schema   %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address  <- address  %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided (or set via ezql_details_*).")
  }

  # -- Infer SQL name if not provided
  if (is.null(sql_name)) {
    sql_name <- r_name
  }

  # -- Validate r_type
  valid_r_types <- c("as.character", "as.numeric", "as.integer", "as.logical", "as.Date", "as.POSIXct")
  if (!(r_type %in% valid_r_types)) {
    stop(glue::glue("Invalid r_type: '{r_type}'. Must be one of: {paste(valid_r_types, collapse = ', ')}"))
  }

  # -- Validate sql_type base
  valid_sql_base_types <- c(
    "int", "float", "decimal", "numeric",
    "bit", "char", "varchar", "nvarchar", "nchar", "text", "ntext",
    "date", "datetime", "datetime2", "smalldatetime", "time",
    "uniqueidentifier", "money", "smallmoney", "binary", "varbinary"
  )
  normalized_sql_type <- tolower(gsub("\\s+", " ", trimws(sql_type)))
  sql_base_type <- sub("^\\s*(\\w+).*", "\\1", normalized_sql_type)

  if (!(sql_base_type %in% valid_sql_base_types)) {
    stop(glue::glue("Invalid sql_type: '{sql_type}'. Base type '{sql_base_type}' is not recognized."))
  }

  # -- Check for existing entries
  rosetta <- ezql_rosetta()

  if (r_name %in% rosetta$r) {
    stop(glue::glue("The r_name '{r_name}' already exists in the rosetta. Use ezql_column_rosetta_alter() instead."))
  }

  if (sql_name %in% rosetta$sql) {
    stop(glue::glue("The sql_name '{sql_name}' already exists in the rosetta. Use ezql_column_rosetta_alter() instead."))
  }

  # -- Add new row and update table
  updated <- rosetta %>%
    add_row(r = r_name, sql = sql_name, sql_type = sql_type, r_type = r_type)

  ezql_edit(
    updated,
    "ezql_rosetta",
    rosetta = updated,
    schema = schema,
    database = database,
    address = address
  )

  message(glue::glue("Added rosetta mapping: {r_name} → {sql_name} [{sql_type}]"))

  message(str_c("Here's the last 5 rows of the revised ezql_rosetta. ",
                "The full table is returned invisibly."))

  updated %>%
    arrange(r == r_name) %>%
    tail(5) %>%
    print()

  invisible(updated)
}

#' Alter a Column Definition in the ezql_rosetta Table
#'
#' Modifies one or more fields (R name, SQL name, types) for a column in the rosetta mapping.
#'
#' @param r_name A string: the existing R column name (used to locate the row).
#' @param new_r_name Optional: new R name to assign.
#' @param new_sql_name Optional: new SQL name to assign.
#' @param new_r_type Optional: updated R coercion (e.g., "as.character").
#' @param new_sql_type Optional: updated SQL type string (e.g., "nvarchar(100)").
#' @param schema Optional: schema name (defaults to ezql_details_schema()).
#' @param database Optional: database name (defaults to ezql_details_db()).
#' @param address Optional: SQL Server address (defaults to ezql_details_add()).
#'
#' @return A tibble containing the full updated `ezql_rosetta()` table (invisibly). A message also displays a preview of the last 5 rows, including the affected entry.
#' @importFrom utils tail
#' @importFrom glue glue
#' @export
ezql_column_rosetta_alter <- function(r_name,
                                      new_r_name = NULL,
                                      new_sql_name = NULL,
                                      new_r_type = NULL,
                                      new_sql_type = NULL,
                                      schema = NULL,
                                      database = NULL,
                                      address = NULL) {

  # -- Resolve connection details
  schema   <- schema   %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address  <- address  %||% ezql_details_add()
  if (is.null(schema) || is.null(database) || is.null(address))
    stop("Schema, database, and address must be provided (or set via ezql_details_*).")

  rosetta <- ezql_rosetta()

  # -- Locate the row to update
  match_row <- which(rosetta$r == r_name)
  if (length(match_row) == 0) {
    stop(glue::glue("No row found for r_name = '{r_name}' in ezql_rosetta."))
  }
  if (length(match_row) > 1) {
    stop(glue::glue("Multiple rows found for r_name = '{r_name}'. Cannot proceed."))
  }

  # -- Validate new_r_name (must not duplicate)
  if (!is.null(new_r_name) && new_r_name != r_name && new_r_name %in% rosetta$r) {
    stop(glue::glue("Cannot rename to '{new_r_name}' — already exists in `r` column."))
  }

  # -- Validate new_sql_name (must not duplicate)
  old_sql_name <- rosetta$sql[match_row]
  if (!is.null(new_sql_name) && new_sql_name != old_sql_name && new_sql_name %in% rosetta$sql) {
    stop(glue::glue("Cannot assign SQL name '{new_sql_name}' — already used in `sql` column."))
  }

  # -- Validate r_type and sql_type if supplied
  if (!is.null(new_r_type)) {
    valid_r <- c("as.character", "as.numeric", "as.integer", "as.logical", "as.Date", "as.POSIXct")
    if (!(new_r_type %in% valid_r)) {
      stop(glue::glue("Invalid new_r_type: '{new_r_type}'. Must be one of: {paste(valid_r, collapse = ', ')}"))
    }
  }
  if (!is.null(new_sql_type)) {
    valid_sql <- c(
      "int", "float", "decimal", "numeric",
      "bit", "char", "varchar", "nvarchar", "nchar", "text", "ntext",
      "date", "datetime", "datetime2", "smalldatetime", "time",
      "uniqueidentifier", "money", "smallmoney", "binary", "varbinary"
    )
    base_type <- tolower(sub("^\\s*(\\w+).*", "\\1", new_sql_type))
    if (!(base_type %in% valid_sql)) {
      stop(glue::glue("Invalid new_sql_type: '{new_sql_type}'. Base type '{base_type}' not recognized."))
    }
  }

  # -- Apply changes
  if (!is.null(new_r_name))     rosetta$r[match_row]        <- new_r_name
  if (!is.null(new_sql_name))   rosetta$sql[match_row]      <- new_sql_name
  if (!is.null(new_r_type))     rosetta$r_type[match_row]   <- new_r_type
  if (!is.null(new_sql_type))   rosetta$sql_type[match_row] <- new_sql_type

  ezql_edit(
    rosetta,
    "ezql_rosetta",
    rosetta = rosetta,
    schema = schema,
    database = database,
    address = address,
    delete_missing_rows = TRUE
  )

  message(glue::glue("Rosetta row for '{r_name}' updated."))

  message(str_c("Here's the last 5 rows of the revised ezql_rosetta. ",
                "The full table is returned invisibly."))

  rosetta %>%
    arrange(dplyr::row_number() == match_row) %>%
    tail(5) %>%
    print()

  invisible(rosetta)
}

#' Delete a Column Definition from the ezql_rosetta Table
#'
#' Removes a row from the ezql_rosetta mapping table, based on the R column name.
#' This operation is **irreversible** — be sure you want to remove the mapping.
#'
#' @param r_name The R-side column name to delete from the rosetta mapping.
#' @param schema Optional: schema name (defaults to ezql_details_schema()).
#' @param database Optional: database name (defaults to ezql_details_db()).
#' @param address Optional: SQL Server address (defaults to ezql_details_add()).
#'
#' @return A tibble containing the full updated `ezql_rosetta()` table (invisibly). A message also displays a preview of the last 5 rows, including the affected entry.
#' @export
#' @importFrom utils tail
#' @importFrom glue glue
ezql_column_rosetta_drop <- function(r_name,
                                     schema = NULL,
                                     database = NULL,
                                     address = NULL) {

  # -- Resolve connection details
  schema   <- schema   %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address  <- address  %||% ezql_details_add()
  if (is.null(schema) || is.null(database) || is.null(address))
    stop("Schema, database, and address must be provided (or set via ezql_details_*).")

  rosetta <- ezql_rosetta()

  match_row <- which(rosetta$r == r_name)
  if (length(match_row) == 0) {
    stop(glue::glue("No row found for r_name = '{r_name}' in ezql_rosetta."))
  }
  if (length(match_row) > 1) {
    stop(glue::glue("Multiple rows found for r_name = '{r_name}'. Cannot delete reliably."))
  }

  # -- Remove the row
  updated_rosetta <- rosetta[-match_row, ]

  ezql_edit(
    updated_rosetta,
    "ezql_rosetta",
    rosetta = updated_rosetta,
    schema = schema,
    database = database,
    address = address,
    delete_missing_rows = TRUE
  )

  message(glue::glue("Deleted rosetta mapping for: '{r_name}'"))
  message(str_c("Here's the last 5 rows of the revised ezql_rosetta. ",
                "The full table is returned invisibly."))
  updated_rosetta %>%
    tail(5) %>%
    print()

  invisible(updated_rosetta)
}


#' Add a New Column to SQL Table and ezql_rosetta Mapping
#'
#' Adds a column to a SQL Server table using `ezql_column_sql_add()`, then registers the mapping
#' in `ezql_rosetta` via `ezql_column_rosetta_add()`. This function ensures consistency:
#'
#' - If the SQL column creation succeeds but the rosetta update fails, the SQL column is
#'   **automatically dropped** to avoid a mismatch.
#'
#' @param table Name of the target SQL table.
#' @param r_name Column name as used in R.
#' @param r_type R coercion function (e.g., "as.character", "as.numeric").
#' @param sql_type Full SQL type (e.g., "nvarchar(100)", "int NOT NULL").
#' @param sql_name Optional. Column name to use in SQL. Defaults to `r_name`.
#' @param backfill_default Logical: if `TRUE`, fills existing rows with the default after adding.
#' @param default Default value for the SQL column. If a character, it will be quoted unless already SQL-safe.
#' @param schema Optional. SQL schema name (default from `ezql_details_schema()`).
#' @param database Optional. SQL database name (default from `ezql_details_db()`).
#' @param address Optional. SQL Server address (default from `ezql_details_add()`).
#'
#' @return Invisibly returns `NULL`. Stops with an informative error message on failure.
#' @export
#' @importFrom rlang %||%
ezql_column_add <- function(table,
                            r_name,
                            r_type,
                            sql_type,
                            sql_name = NULL,
                            backfill_default = FALSE,
                            default = NULL,
                            schema = NULL,
                            database = NULL,
                            address = NULL) {

  # ---- Resolve connection ----
  schema   <- schema   %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address  <- address  %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided (or set via ezql_details_*).")
  }

  # ---- Set default sql_name to match r_name ----
  sql_name <- sql_name %||% r_name

  # ---- Pre-validate rosetta entry ----
  rosetta <- ezql_rosetta()
  if (r_name %in% rosetta$r) stop("The r_name already exists in the rosetta. Use ezql_column_alter instead.")
  if (sql_name %in% rosetta$sql) stop("The sql_name already exists in the rosetta. Use ezql_column_alter instead.")

  # ---- Determine NOT NULL ----
  is_not_null <- stringr::str_detect(stringr::str_to_lower(sql_type), "not null")

  # ---- Proceed and fail safely ----
  tryCatch({
    ezql_column_sql_add(
      table            = table,
      column_name      = sql_name,
      data_type        = sql_type,
      null             = !is_not_null,
      default          = default,
      backfill_default = backfill_default,
      schema           = schema,
      database         = database,
      address          = address
    )

    ezql_column_rosetta_add(
      table     = table,
      r_name    = r_name,
      r_type    = r_type,
      sql_type  = sql_type,
      sql_name  = sql_name,
      schema    = schema,
      database  = database,
      address   = address
    )

  }, error = function(e) {
    # Attempt rollback
    suppressWarnings(try(
      ezql_column_sql_drop(table, sql_name, schema, database, address),
      silent = TRUE
    ))
    stop("Failed to add column '", r_name, "': ", e$message)
  })

  invisible(NULL)
}


#' Drop a Column from Both SQL Table and Rosetta Mapping
#'
#' Removes a column from a SQL Server table and the `ezql_rosetta` mapping.
#' If the column has a default constraint in SQL Server, it is automatically dropped first.
#'
#' Provides a final summary message using `cli` indicating what actions were taken.
#'
#' @param table The name of the SQL table (character string).
#' @param r_name The name of the column in the R (rosetta) mapping.
#' @param schema Optional: schema name. Defaults to `ezql_details_schema()`.
#' @param database Optional: database name. Defaults to `ezql_details_db()`.
#' @param address Optional: SQL Server address. Defaults to `ezql_details_add()`.
#'
#' @return Invisibly returns `NULL`. Stops on failure. Prints a `cli`-styled summary.
#' @export
#' @importFrom cli cli_alert_success cli_alert_warning cli_alert_info cli_h1
ezql_column_drop <- function(table,
                             r_name,
                             schema = NULL,
                             database = NULL,
                             address = NULL) {

  schema   <- schema   %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address  <- address  %||% ezql_details_add()

  if (is.null(schema) || is.null(database) || is.null(address)) {
    stop("Schema, database, and address must be provided (or set via ezql_details_*).")
  }

  rosetta <- ezql_rosetta()
  column_in_rosetta <- r_name %in% rosetta$r

  # Derive SQL name
  sql_name <- if (column_in_rosetta) {
    rosetta$sql[match(r_name, rosetta$r)]
  } else {
    NULL
  }

  # 1. Drop from SQL table (if found)
  sql_dropped <- FALSE
  if (!is.null(sql_name)) {
    tryCatch({
      ezql_column_sql_drop(table, sql_name, schema = schema, database = database, address = address)
      sql_dropped <- TRUE
    }, error = function(e) {
      cli::cli_alert_warning("SQL column {.field {sql_name}} could not be dropped: {e$message}")
    })
  } else {
    cli::cli_alert_warning("Column {.field {r_name}} not found in rosetta — SQL drop skipped.")
  }

  # 2. Drop from rosetta mapping (if found)
  rosetta_dropped <- FALSE
  if (column_in_rosetta) {
    tryCatch({
      ezql_column_rosetta_drop(r_name, schema = schema, database = database, address = address)
      rosetta_dropped <- TRUE
    }, error = function(e) {
      cli::cli_alert_warning("Failed to drop rosetta mapping for {.field {r_name}}: {e$message}")
    })
  } else {
    cli::cli_alert_warning("Column {.field {r_name}} not found in rosetta — mapping not removed.")
  }

  # Summary
  cli::cli_h1("Column Drop Summary")
  if (sql_dropped)     cli::cli_alert_success("Removed SQL column {.field {sql_name}} from table {.table {table}}.")
  if (rosetta_dropped) cli::cli_alert_success("Removed {.field {r_name}} from rosetta mapping.")

  if (!sql_dropped && !rosetta_dropped) {
    cli::cli_alert_warning("No action taken — column was not found in SQL table or rosetta.")
  }

  invisible(NULL)
}


#' Alter a Column in Both SQL and Rosetta Mapping
#'
#' This function updates a column in both the SQL Server table and the associated `ezql_rosetta` mapping.
#' It ensures synchronization between the physical schema and metadata by performing the SQL rename/type/default change first,
#' followed by the rosetta update. If the rosetta update fails, it attempts to roll back the SQL rename.
#'
#' @param table The name of the SQL table where the column resides. This is a required argument.
#' @param r_name The current R-side column name in the rosetta mapping (used to locate the row).
#' @param new_r_name Optional. New R column name to assign in rosetta.
#' @param new_sql_name Optional. New SQL column name to assign in the SQL table.
#' @param new_r_type Optional. New R coercion function (e.g., "as.character") to assign in rosetta.
#' @param new_sql_type Optional. New SQL type declaration (e.g., "nvarchar(100)") to apply in SQL.
#' @param default Optional. SQL default value (as raw SQL, e.g., 'Unknown' or GETDATE()).
#' @param schema Optional. Schema name (defaults to `ezql_details_schema()`).
#' @param database Optional. Database name (defaults to `ezql_details_db()`).
#' @param address Optional. SQL Server address (defaults to `ezql_details_add()`).
#'
#' @return A named list indicating which changes were successfully made: `list(sql_altered = TRUE/FALSE, rosetta_updated = TRUE/FALSE)`.
#' @export
ezql_column_alter <- function(table,
                              r_name,
                              new_r_name = NULL,
                              new_sql_name = NULL,
                              new_r_type = NULL,
                              new_sql_type = NULL,
                              default = NULL,
                              schema = NULL,
                              database = NULL,
                              address = NULL) {

  # Resolve connection details
  schema   <- schema   %||% ezql_details_schema()
  database <- database %||% ezql_details_db()
  address  <- address  %||% ezql_details_add()

  rosetta <- ezql_rosetta()
  match_row <- which(rosetta$r == r_name)
  if (length(match_row) != 1) {
    stop(glue::glue("Found {length(match_row)} rows matching '{r_name}' in rosetta. Must match exactly one."))
  }

  old_sql_name <- rosetta$sql[match_row]

  changes <- list(sql_altered = FALSE, rosetta_updated = FALSE)

  # Try altering SQL schema
  if (!purrr::every(list(new_sql_name, new_sql_type, default), is.null)) {
    tryCatch({
      ezql_column_sql_alter(
        table       = table,
        column_name = old_sql_name,
        rename_to   = new_sql_name,
        new_type    = new_sql_type,
        default     = default,
        schema      = schema,
        database    = database,
        address     = address
      )
      changes$sql_altered <- TRUE
    }, error = function(e) {
      stop("SQL schema update failed: ", e$message)
    })
  }

  # Try altering rosetta mapping
  tryCatch({
    ezql_column_rosetta_alter(
      r_name         = r_name,
      new_r_name     = new_r_name,
      new_sql_name   = new_sql_name,
      new_r_type     = new_r_type,
      new_sql_type   = new_sql_type,
      schema         = schema,
      database       = database,
      address        = address
    )
    changes$rosetta_updated <- TRUE
  }, error = function(e) {
    # Attempt rollback if rename failed
    if (changes$sql_altered && !is.null(new_sql_name)) {
      try(ezql_column_sql_alter(
        table       = table,
        column_name = new_sql_name,
        rename_to   = old_sql_name,
        schema      = schema,
        database    = database,
        address     = address
      ), silent = TRUE)
    }
    stop("Rosetta update failed: ", e$message)
  })

  cli::cli_alert_success("Column '{r_name}' altered in table '{table}' and rosetta mapping.")
  return(invisible(changes))
}
