% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ezql_column.R
\name{ezql_column_sql_alter}
\alias{ezql_column_sql_alter}
\title{Alter (or Rename) a Column in a SQL Table
*(data‑type, default, and **rename** – nullability change currently disabled)*}
\usage{
ezql_column_sql_alter(
  table,
  column_name,
  rename_to = NULL,
  new_type = NULL,
  default = NULL,
  schema = NULL,
  database = NULL,
  address = NULL
)
}
\arguments{
\item{table}{Table name.}

\item{column_name}{Current column name.}

\item{rename_to}{Optional. **New** column name.  NULL → no rename.}

\item{new_type}{Optional. New SQL data type, e.g., "VARCHAR(150)".}

\item{default}{Optional. Default value **as raw SQL** ('active', 0, GETDATE()).
NULL → drop existing default without adding a new one.}

\item{schema}{Defaults to ezql_details_schema().}

\item{database}{Defaults to ezql_details_db().}

\item{address}{Defaults to ezql_details_add().}
}
\value{
Invisible NULL (stops on any SQL error).
}
\description{
This helper lets you:
• **rename** a column (old_name → new_name) *via* sp_rename()
• change its **data type** (e.g., VARCHAR(50) → VARCHAR(100))
• (re)create a **DEFAULT** constraint.
}
\details{
**What it does *not* do (yet)**
Switching NULL ↔ NOT NULL is still disabled pending a rock‑solid temporal
solution (see placeholder note below).

*Placeholder note (2025‑05‑13):*  We previously attempted a full
nullability routine for system‑versioned tables but hit edge‑cases where
SQL Server silently reverted the flag.  Future‑Geordie can revisit once we
have a bullet‑proof approach.
}
