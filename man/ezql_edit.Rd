% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sql.R
\name{ezql_edit}
\alias{ezql_edit}
\title{Edit a SQL Table}
\usage{
ezql_edit(
  df,
  table,
  schema = NULL,
  database = NULL,
  address = NULL,
  delete_missing_rows = FALSE
)
}
\arguments{
\item{df}{A data frame containing the new data for the SQL table.}

\item{table}{A string specifying the name of the target SQL table.}

\item{schema}{A string specifying the schema of the table. Defaults to the value
retrieved from \code{ezql_details_schema()}.}

\item{database}{A string specifying the database name. Defaults to the value
retrieved from \code{ezql_details_db()}.}

\item{address}{A string specifying the server address. Defaults to the value
retrieved from \code{ezql_details_add()}.}

\item{delete_missing_rows}{Logical. If \code{TRUE}, rows in the SQL table but not
in the data frame are deleted. Defaults to \code{FALSE}.}
}
\value{
A list summarizing the number of rows added, updated, and deleted.
}
\description{
A wrapper function that combines adding, updating, and deleting rows in a SQL table
based on a data frame. Allows for a comprehensive table update in one step.
}
\examples{
\dontrun{
# Edit a table by adding, updating, and optionally deleting rows
result <- ezql_edit(
  df = new_data,
  table = "my_table",
  schema = "dbo",
  database = "my_database",
  address = "my_server",
  delete_missing_rows = TRUE
)
print(result)
}

}
\seealso{
\code{\link{ezql_add_data}}, \code{\link{ezql_alter_data}}, \code{\link{ezql_delete_data}}
}
