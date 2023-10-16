import pyodbc


def create_key_stage(conn, stage_schema, schema_name, table_name, identity):
    "create KeyStage table if it doesnt exist"
    cnxn = pyodbc.connect(conn, autocommit=True)
    crsr = cnxn.cursor()

    get_identity_data_type_sql = f"""
            SELECT DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name}'
            AND TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{identity}'
        """
    crsr.execute(get_identity_data_type_sql)
    identity_data_type = crsr.fetchone()[0]

    # Construct column names and data types for the staging table
    new_identity_column = f"New_{identity}"
    source_identity_column = f"Source_{identity}"

    # Clean up the staging table if needed
    cleanup_staging_table_sql = f"""
        DROP TABLE IF EXISTS [{stage_schema}].[KeyStage];
    """
    crsr.execute(cleanup_staging_table_sql)

    # Create a permanent staging table if it doesn't already exist
    create_staging_table_sql = f"""
        CREATE TABLE [{stage_schema}].[KeyStage] (
            [{new_identity_column}] {identity_data_type},
            [{source_identity_column}] {identity_data_type}
        )
    """
    crsr.execute(create_staging_table_sql)

    crsr.close()
    cnxn.close()

    return new_identity_column, source_identity_column


def update_new_pk_in_stage(conn, stage_schema, table_name, key_arrays):
    "Update the New_ column in the STAGE schema"
    cnxn = pyodbc.connect(conn, autocommit=True)
    crsr = cnxn.cursor()

    # Parse the key_array
    column_name = key_arrays["column_name"]
    inserted_identity_values = key_arrays["inserted_identity_values"]
    source_identity_values = key_arrays["source_identity_values"]

    # Construct and execute dynamic SQL statements to update New_ column
    for inserted_id, source_id in zip(inserted_identity_values, source_identity_values):
        update_sql = f"""
        UPDATE [{stage_schema}].[{table_name}]
        SET New_{column_name} = ?
        WHERE {column_name} = ?
        """
        crsr.execute(update_sql, inserted_id, source_id)

    crsr.close()
    cnxn.close()


def update_fks_in_stage(conn, stage_schema, table_name, fks_list):
    """"""
    cnxn = pyodbc.connect(conn, autocommit=True)
    crsr = cnxn.cursor()

    for fk in fks_list:
        update_query = f"""
            UPDATE [{stage_schema}].[{table_name}]
            SET New_{fk['parent_column']} =
            COALESCE(parent.New_{fk['referenced_column']}, parent.{fk['referenced_column']})
            FROM [{stage_schema}].[{table_name}] stage
            INNER JOIN [{stage_schema}].[{fk['referenced_table']}] parent
            ON stage.{fk['parent_column']} = parent.{fk['referenced_column']}
        """
        crsr.execute(update_query)
        cnxn.commit()

        print(f"Updated Foreign Key [{fk['name']}]")

    crsr.close()
    cnxn.close()
