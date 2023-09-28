import pyodbc


def create_stage_schema(conn):
    "create stage schema if it doesnt exist"
    cnxn = pyodbc.connect(conn, autocommit=True)
    crsr = cnxn.cursor()
    crsr.execute("SELECT * FROM sys.schemas WHERE name = 'STAGE'")
    result = crsr.fetchall()
    if result is None or len(result) == 0:
        print("Creating Schema: STAGE")
        crsr.execute("CREATE SCHEMA STAGE")
    else:
        print("STAGE schema already exists")
    crsr.close()
    cnxn.close()


def create_stage_table(conn, table_name, recreate=False):
    "create table in STAGE schema from it's dbo equivalant"
    cnxn = pyodbc.connect(conn, autocommit=True)
    crsr = cnxn.cursor()

    create_table_query = (
        f"SELECT TOP 0 * INTO STAGE.[{table_name}] FROM dbo.[{table_name}]"
    )

    # Check if the table exists
    crsr.execute(
        f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'STAGE' AND TABLE_NAME = '{table_name}'"
    )
    result = crsr.fetchall()
    if result is None or len(result) == 0:
        print(f"Creating Table: STAGE.[{table_name}]")
        crsr.execute(create_table_query)
    else:
        if recreate is True:
            print(f"Dropping and Recreating Table: [STAGE].[{table_name}]")
            crsr.execute(f"DROP TABLE STAGE.[{table_name}]")
            crsr.execute(create_table_query)
        else:
            print(
                f"Table [STAGE].[{table_name}] already exists and recreate is False, skipping table creation."
            )

    crsr.close()
    cnxn.close()


def create_stage_table_pk(conn, table_name, pk_column_list):
    "create the same PK on the stage table"
    cnxn = pyodbc.connect(conn, autocommit=True)
    crsr = cnxn.cursor()

    pk_columns = ", ".join(item["PrimaryKeyName"] for item in pk_column_list)
    create_pk_query = f"""
    ALTER TABLE [STAGE].[{table_name}] ADD CONSTRAINT PK_STAGE_{table_name}
    PRIMARY KEY ({pk_columns})
    """

    crsr.execute(create_pk_query)
    crsr.close()


def create_stage_table_newpk(conn, table_name):
    "create new column on stage table of the pk column"
    cnxn = pyodbc.connect(conn, autocommit=True)
    crsr = cnxn.cursor()

    # Retrieve primary key column information from sys schema
    query = f"""
    SELECT c.name AS ColumnName, ty.name AS DataType
    FROM sys.columns c
    INNER JOIN sys.tables t ON c.object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    INNER JOIN sys.types ty ON c.system_type_id = ty.system_type_id AND c.user_type_id = ty.user_type_id
    INNER JOIN sys.index_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE s.name = 'STAGE'
        AND t.name = '{table_name}'
        AND ic.index_id = 1  -- Assuming the primary key index is always 1
"""
    crsr = cnxn.cursor()
    crsr.execute(query)
    pk_info = crsr.fetchall()

    new_column_prefix = "New_"  # Prefix for the new columns

    if pk_info:
        for info in pk_info:
            new_column_name = f"{new_column_prefix}{info.ColumnName}"
            data_type = info.DataType

            # Construct and execute the ALTER TABLE query for each new column
            alter_query = f"""
                ALTER TABLE [STAGE].[{table_name}]
                ADD [{new_column_name}] {data_type} NULL
            """
            crsr.execute(alter_query)
            print(
                f"New column '{new_column_name}' added to '[STAGE].[{table_name}]' with data type '{data_type}'."
            )

        cnxn.commit()

    crsr.close()
    cnxn.close()


def create_stage_table_fks(conn, schema_name, table_name, foreign_keys):
    "create new columns on stage table for the fk columns"
    cnxn = pyodbc.connect(conn, autocommit=True)
    crsr = cnxn.cursor()

    new_column_prefix = "New_"  # Prefix for the new columns

    for fk in foreign_keys:
        column_name = fk["parent_column"]

        # Get the data type of the foreign key column
        data_type_query = f"""
            SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name}'
                AND TABLE_NAME = '{table_name}'
                AND COLUMN_NAME = '{column_name}'
        """
        crsr.execute(data_type_query)
        data_type = crsr.fetchone()[0]

        new_column_name = f"{new_column_prefix}{column_name}"

        # Check if the new column already exists
        check_query = f"""
            SELECT COUNT(*)
            FROM sys.columns c
            INNER JOIN sys.tables t ON c.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'STAGE'
                AND t.name = '{table_name}'
                AND c.name = '{new_column_name}'
        """
        crsr.execute(check_query)
        column_exists = crsr.fetchone()[0]

        if not column_exists:
            # Construct and execute the ALTER TABLE query for each new column
            alter_query = f"""
                ALTER TABLE [STAGE].[{table_name}]
                ADD [{new_column_name}] {data_type} NULL
            """
            crsr.execute(alter_query)
            print(
                f"New column '{new_column_name}' added to '[STAGE].[{table_name}]' with data type '{data_type}'."
            )
        else:
            print(
                f"Column '{new_column_name}' already exists in table '[STAGE].[{table_name}]'."
            )

    cnxn.commit()

    crsr.close()
    cnxn.close()
