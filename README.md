# ezekiel

> A friendly, opinionated R toolkit for working with SQL Server.

`ezekiel` wraps [`RODBC`](https://cran.r-project.org/package=RODBC) in a set of
consistent, pipe-friendly `ezql_*` functions that make everyday SQL Server work
feel like working with regular R data frames. It handles connection details,
reading and writing tables, keeping a table in sync with a data frame, managing
columns and schemas, and querying system-versioned (temporal) history — all
while returning tidy [tibbles](https://tibble.tidyverse.org/).

## Features

- **Stored connection details** — set your server address, database, and schema
  once (per session or persisted to your `.Rprofile`) and let every function
  pick them up automatically.
- **Read and write tables as data frames** — fetch a table with `ezql_get()` /
  `ezql_table()`, run arbitrary queries with `ezql_query()`, and create, replace,
  or drop tables from a data frame.
- **One-step table sync** — `ezql_edit()` compares a data frame to a SQL table
  and adds, updates, and (optionally) deletes rows to make them match.
- **The "rosetta" mapping** — keep a small mapping table in your database that
  translates between R column names, SQL column names, and data types, so reads
  and writes are renamed and coerced consistently.
- **Column and schema management** — add, alter, and drop columns while keeping
  the rosetta mapping in sync.
- **Temporal tables** — convert a table to a system-versioned temporal table and
  query its state "as of" any point in time.
- **Optional logging** — record operations to a log file configured via your
  `.Rprofile`.

## Installation

`ezekiel` is not on CRAN. Install the development version from GitHub:

```r
# install.packages("remotes")
remotes::install_github("mcruer/ezekiel")
```

### Requirements

- An ODBC driver and connection to a SQL Server instance (the package uses
  `RODBC` under the hood).
- A few of the maintainer's own packages are also required: `gplyr` and
  `listful`. Install them from GitHub if you don't have them:

  ```r
  remotes::install_github("mcruer/gplyr")
  remotes::install_github("mcruer/listful")
  ```

## Getting started

### 1. Set your connection details

Most functions accept `schema`, `database`, and `address` arguments, but you
rarely need to pass them. Instead, store them once:

```r
library(ezekiel)

# For the current session only:
ezql_set_details(
  schema   = "dbo",
  database = "my_database",
  address  = "my_server"
)

# Or persist them to your .Rprofile so they load in every session
# (restart R afterwards to apply):
ezql_details_to_rprofile(
  schema   = "dbo",
  database = "my_database",
  address  = "my_server"
)
```

You can inspect the stored values with `ezql_details()`, `ezql_details_db()`,
`ezql_details_schema()`, and `ezql_details_add()`.

### 2. Read data

```r
# List the tables available in the current schema/database
ezql_list_tables()

# Fetch an entire table as a tibble
df <- ezql_get("my_table")

# Fetch a table with rosetta renaming / type coercion applied
df <- ezql_table("my_table")

# Run an arbitrary SQL query
results <- ezql_query("SELECT TOP 10 * FROM dbo.my_table")
```

### 3. Write data

```r
# Create a new table from a data frame (errors if it already exists)
ezql_new(df, "my_table", primary_key = "ID")

# Replace an existing table (errors if it does not exist)
ezql_replace(df, "my_table", primary_key = "ID")

# Drop a table
ezql_drop("my_table")
```

### 4. Keep a table in sync with a data frame

`ezql_edit()` is the workhorse for incremental updates. It compares your data
frame to the table and applies the differences in one call:

```r
result <- ezql_edit(
  df    = updated_data,
  table = "my_table",
  delete_missing_rows = TRUE   # also delete rows no longer in `df`
)
# result reports how many rows were added, altered, and deleted
```

To preview the changes without applying them, use `ezql_check_table()`, which
returns the rows that would be added, altered, and deleted.

## The rosetta mapping

Many functions accept a `rosetta` argument (defaulting to `ezql_rosetta()`,
which reads a table named `ezql_rosetta` from your database). The rosetta is a
small mapping table with columns such as:

| `r`            | `sql`        | `r_type`        | `sql_type`     |
|----------------|--------------|-----------------|----------------|
| `local_name`   | `DbColumn`   | `as.character`  | `nvarchar(100)`|

This lets `ezekiel`:

- rename columns between R and SQL conventions on read and write, and
- coerce each column to the correct R type when reading and the correct SQL type
  when creating columns.

The `ezql_column_*` family adds, alters, and drops columns while keeping both
the SQL table and the rosetta mapping consistent — for example, `ezql_column_add()`
adds the SQL column and registers the mapping, rolling back the SQL change if the
mapping update fails.

## Temporal tables

`ezekiel` supports SQL Server system-versioned temporal tables:

```r
# Convert an existing table into a temporal (system-versioned) table
ezql_make_temporal("my_table", primary_key_column = "ID")

# Retrieve the state of the table as of a given date
historical <- ezql_get_as_of("my_table", as_of = "2025-03-01")

# ezql_table() also accepts an `as_of` argument
snapshot <- ezql_table("my_table", as_of = "2025-03-01")
```

## Logging (optional)

Configure a log file path once and `ezekiel` can record operations to it:

```r
ezql_log_path_to_rprofile("~/ezekiel.log")  # persist to .Rprofile, then restart R
ezql_log_path()                              # check the configured path
```

## Function reference

A selection of the exported functions, grouped by purpose:

- **Connection details:** `ezql_set_details()`, `ezql_details()`,
  `ezql_details_add()`, `ezql_details_db()`, `ezql_details_schema()`,
  `ezql_details_to_rprofile()`, `ezql_connect()`
- **Reading:** `ezql_get()`, `ezql_get_as_of()`, `ezql_table()`, `ezql_query()`,
  `ezql_list_tables()`, `ezql_table_names()`, `ezql_table_exists()`,
  `ezql_full_table_name()`
- **Writing / table lifecycle:** `ezql_new()`, `ezql_replace()`, `ezql_drop()`,
  `ezql_edit()`, `ezql_add_data()`, `ezql_alter_data()`, `ezql_delete_data()`,
  `ezql_check_table()`, `ezql_fix()`, `ezql_change_names()`,
  `ezql_set_data_types()`
- **Primary keys & temporal:** `ezql_assign_primary_key()`,
  `ezql_primary_key_name()`, `ezql_primary_key_data()`, `ezql_make_temporal()`
- **Columns & rosetta:** `ezql_rosetta()`, `ezql_column_add()`,
  `ezql_column_alter()`, `ezql_column_drop()`, `ezql_column_sql_add()`,
  `ezql_column_sql_alter()`, `ezql_column_sql_drop()`,
  `ezql_column_rosetta_add()`, `ezql_column_rosetta_alter()`,
  `ezql_column_rosetta_drop()`
- **Logging:** `ezql_log_path()`, `ezql_log_path_to_rprofile()`

See the package help (e.g. `?ezql_edit`) for full documentation of each function.

## License

MIT © ezekiel authors. See [LICENSE.md](LICENSE.md).
