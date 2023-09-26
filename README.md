# MS SQL Data Migration

The goal is to merge the data of one database into another (of identical schemas), accounting for all the primary and foreign keys created by auto-incrementing identity columns.

The scope of this data migration covers three types of tables:

1. Identity Primary Key.  A single Id column that auto increments makes up the primary key.
1. Composite Primary Key.  The PK is a combination of multiple foreign keys.
1. Heap or PK without identity.  Some tables have a PK but it's of a Code (varchar) or GUID, not managed by an auto-incrementing identity.

## Local Env Setup

1. python -m venv .venv/
1. source .venv/bin/activate
1. python -m pip install -r ./requirements.txt

  - Note: on Apple Silicon use `brew install unixodbc` and `pip install --no-binary :all: pyodbc`
  - Also [https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver16#microsoft-odbc-18]

## Goals and Assumptions

1. Manually add tables in the tables.json config file into the appropriate waves
1. Copy Data from Source DB into STAGE schema on Destination DB
1. Add a new column onto every table in STAGE schema for New_PkId as well as each FK column
1. Loop through each FK for table and update it's key based on the New_PkId value of referenced table
1. Copy final values from STAGE schema into final table

## Notes

- Data moves in waves.  For example:
  - Wave 1 contains tables with no FKs.  Top of hierarchy.
  - Wave 2 contains tables with FKs only to those tables in wave 1.
  - Continue this pattern for all tables
- Loop through the following steps for each table in current wave
  - Copy table from source to a staging area on destination DB
  - Add a New_ column to the table with same data type as the original column
  - Insert the records of stage table into destination table
  - Update the stage table with the New_ values that were created during destination insert
