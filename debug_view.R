# Script to inspect the DistrictSchoolBoard view definition

library(ezekiel)

# Connect to database
conn <- ezql_connect()

# Get the view definition
view_definition <- RODBC::sqlQuery(conn, "
  SELECT
    OBJECT_DEFINITION(OBJECT_ID('capitalProjects.DistrictSchoolBoard')) AS ViewDefinition
")

print("=== VIEW DEFINITION ===")
print(view_definition)

# Also check what columns the view thinks it has
view_columns <- RODBC::sqlQuery(conn, "
  SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME = 'DistrictSchoolBoard'
    AND TABLE_SCHEMA = 'capitalProjects'
  ORDER BY ORDINAL_POSITION
")

print("\n=== COLUMNS VIEW EXPECTS ===")
print(view_columns)

# Compare to what the base table actually has
actual_columns <- RODBC::sqlQuery(conn, "
  SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME = 'Boards'
    AND TABLE_SCHEMA = 'capitalProjects'
  ORDER BY ORDINAL_POSITION
")

print("\n=== ACTUAL COLUMNS IN BASE TABLE ===")
print(actual_columns)

RODBC::odbcClose(conn)
