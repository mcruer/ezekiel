% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ezql_column.R
\name{ezql_column_sql_drop}
\alias{ezql_column_sql_drop}
\title{Drop a Column from a SQL Table (with automatic constraint removal)}
\usage{
ezql_column_sql_drop(
  table,
  column_name,
  schema = NULL,
  database = NULL,
  address = NULL
)
}
\arguments{
\item{table}{A string specifying the name of the table to modify.}

\item{column_name}{A string specifying the name of the column to drop.}

\item{schema}{A string specifying the schema. Defaults to NULL and uses the value from ezql_details_schema().}

\item{database}{A string specifying the database name. Defaults to NULL and uses the value from ezql_details_db().}

\item{address}{A string specifying the SQL server address. Defaults to NULL and uses the value from ezql_details_add().}
}
\value{
A list with the result of the column drop query.
}
\description{
Removes a column from a specified SQL Server table. If the column has an associated
default constraint (which SQL Server automatically generates when you define a default),
this function will first detect and drop the constraint before attempting to drop the column.
}
