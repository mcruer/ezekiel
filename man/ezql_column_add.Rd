% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ezql_column.R
\name{ezql_column_add}
\alias{ezql_column_add}
\title{Add a New Column to SQL Table and ezql_rosetta Mapping}
\usage{
ezql_column_add(
  table,
  r_name,
  r_type,
  sql_type,
  sql_name = NULL,
  backfill_default = FALSE,
  default = NULL,
  schema = NULL,
  database = NULL,
  address = NULL
)
}
\arguments{
\item{table}{Name of the target SQL table.}

\item{r_name}{Column name as used in R.}

\item{r_type}{R coercion function (e.g., "as.character", "as.numeric").}

\item{sql_type}{Full SQL type (e.g., "nvarchar(100)", "int NOT NULL").}

\item{sql_name}{Optional. Column name to use in SQL. Defaults to `r_name`.}

\item{backfill_default}{Logical: if `TRUE`, fills existing rows with the default after adding.}

\item{default}{Default value for the SQL column. If a character, it will be quoted unless already SQL-safe.}

\item{schema}{Optional. SQL schema name (default from `ezql_details_schema()`).}

\item{database}{Optional. SQL database name (default from `ezql_details_db()`).}

\item{address}{Optional. SQL Server address (default from `ezql_details_add()`).}
}
\value{
Invisibly returns `NULL`. Stops with an informative error message on failure.
}
\description{
Adds a column to a SQL Server table using `ezql_column_sql_add()`, then registers the mapping
in `ezql_rosetta` via `ezql_column_rosetta_add()`. This function ensures consistency:
}
\details{
- If the SQL column creation succeeds but the rosetta update fails, the SQL column is
  **automatically dropped** to avoid a mismatch.
}
