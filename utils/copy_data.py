from utils.update_keys import create_key_stage, update_new_pk_in_stage
from utils.Table import Table


def copy_src_table_to_stage(
    src_conn,
    dest_conn,
    table: Table,
):
    "copy a tables data from src_conn to dest_conn in stage schema"
    src_crsr = src_conn.cursor()

    print(f"Starting source to stage table copy of [{table.table_name}]...")

    placeholders = ",".join(["?" for _ in table.column_list.split(",")])

    quoted_stage_name = table.quoted_stage_name()
    quoted_full_name = table.quoted_full_name()

    # Get table data
    get_data_sql = f"SELECT {table.column_list} FROM {quoted_full_name}"
    src_crsr.execute(get_data_sql)
    records = src_crsr.fetchall()

    src_crsr.close()

    dest_crsr = dest_conn.cursor()

    # Insert records into STAGE table
    if table.identity:
        dest_crsr.execute(f"SET IDENTITY_INSERT {quoted_stage_name} ON")
    dest_crsr.fast_executemany = True
    sql = (
        f"INSERT INTO {quoted_stage_name} ({table.column_list}) VALUES ({placeholders})"
    )
    dest_crsr.executemany(sql, records)
    if table.identity:
        dest_crsr.execute(f"SET IDENTITY_INSERT {quoted_stage_name} OFF")

    dest_crsr.close()


def merge_identity_table_data(conn, table: Table, column_list):
    "take data from stage and insert it into destination tables returning PK values to stage"
    crsr = conn.cursor()

    columns = column_list.split(",")

    quoted_stage_name = table.quoted_stage_name()
    quoted_full_name = table.quoted_full_name()

    new_identity_column, source_identity_column = create_key_stage(
        conn=conn,
        table=table,
    )

    # build the WHEN condition based on uniques
    if table.uniques:
        when_conditions = []
        for constraint_name, uq_columns in table.uniques.items():
            # The PK is technically a UNIQUE constraint, but we need to ignore it
            if not all(column in table.identity for column in uq_columns):
                # Create a list of conditions for each column in the unique constraint
                column_conditions = []
                for col in uq_columns:
                    # Check that the source column is not NULL
                    column_conditions.append(f"source.[{col}] IS NOT NULL")

                combined_condition = " AND ".join(column_conditions)

                # Include the duplicate checking logic using NOT EXISTS
                duplicate_check = f"""
                NOT EXISTS (SELECT 1
                FROM {quoted_full_name} AS existing
                WHERE {' AND '.join(f'existing.[{col}] = source.[{col}]' for col in uq_columns)}
                )
                """

                # Combine the source column condition and duplicate check with AND
                full_condition = f"({combined_condition}) AND {duplicate_check}"

                # Add the full condition to the list of when_conditions
                when_conditions.append(full_condition)

        # Combine all conditions with OR since any of them can apply
        if when_conditions:
            when_condition = f"WHEN NOT MATCHED AND {' AND '.join(when_conditions)}"
        else:
            when_condition = "WHEN NOT MATCHED"
    else:
        when_condition = "WHEN NOT MATCHED"

    # Perform the MERGE operation with OUTPUT to the StagingTable
    merge_query = f"""
    MERGE INTO {quoted_full_name} AS target
    USING {quoted_stage_name} AS source
    ON 1 = 0  -- Ensures the INSERT part of the MERGE is executed for all rows
    {when_condition} THEN
        INSERT ({', '.join(columns)})
        VALUES ({', '.join('source.' + col for col in columns)})
    OUTPUT inserted.{table.identity} AS [{new_identity_column}], source.{table.identity} AS [{source_identity_column}]
    INTO [{table.stage_schema}].[KeyStage];
    """
    crsr.execute(merge_query)

    # Retrieve data from the StagingTable
    select_staging_data_sql = f"""
        SELECT [{new_identity_column}], [{source_identity_column}]
        FROM [{table.stage_schema}].[KeyStage]
    """
    crsr.execute(select_staging_data_sql)

    # Fetch the results into Python variables
    key_arrays = {
        "column_name": table.identity,
        "inserted_identity_values": [],
        "source_identity_values": [],
    }

    for row in crsr:
        key_arrays["inserted_identity_values"].append(row[0])
        key_arrays["source_identity_values"].append(row[1])

    update_new_pk_in_stage(
        conn=conn,
        table=table,
        key_arrays=key_arrays,
    )

    # Identify any remaining null PKs that didnt get updated above
    # and assume these are duplicates from a UNIQUE constraint so set the New_
    # PK equal to the original PK
    # TODO: There's probably a better way to do this, might fix later
    unique_null_pks_sql = f"""
    UPDATE {quoted_stage_name}
    SET New_{table.identity} = {table.identity}
    WHERE New_{table.identity} IS NULL
    """
    crsr.execute(unique_null_pks_sql)

    crsr.close()


def merge_composite_table_data(conn, table: Table):
    "take composite pk data from stage and insert it into destination table"
    print(f"Merging composite table: {table.table_name}")
    crsr = conn.cursor()

    columns = table.column_list.split(",")

    quoted_stage_name = table.quoted_stage_name()
    quoted_full_name = table.quoted_full_name()

    # Construct the list of PK columns in the format:
    # "source.New_column1 = target.column1 AND source.New_column2 = target.column2"
    pk_conditions = []
    values_columns = []
    for pk_col in table.pk_column_list:
        col_name = pk_col["PrimaryKeyName"]
        if col_name in columns:
            pk_conditions.append(f"source.New_{col_name} = target.{col_name}")
            values_columns.append(col_name)

    pk_conditions = " AND ".join(pk_conditions)

    # Construct the VALUES part
    values_part = ", ".join(
        f"source.New_{col}" if col in values_columns else f"source.{col}"
        for col in columns
    )

    merge_query = f"""
    MERGE INTO {quoted_full_name} AS target
    USING {quoted_stage_name} AS source
    ON {pk_conditions}
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(columns)})
        VALUES ({values_part});
    """
    crsr.execute(merge_query)

    crsr.close()


def merge_unique_table_data(conn, table: Table):
    "Merge unique PK table data from stage into destination table"
    print(f"Merging unique table: {table.table_name}")
    crsr = conn.cursor()

    quoted_stage_name = table.quoted_stage_name()
    quoted_full_name = table.quoted_full_name()

    columns = table.column_list.split(",")

    # Construct the list of PK columns in the format:
    # "source.column1 = target.column1 AND source.column2 = target.column2"
    pk_conditions = []
    values_columns = []
    for pk_col in table.pk_column_list:
        col_name = pk_col["PrimaryKeyName"]
        if col_name in columns:
            pk_conditions.append(f"source.{col_name} = target.{col_name}")
            values_columns.append(col_name)

    pk_conditions = " AND ".join(pk_conditions)

    # Construct the VALUES part
    values_part = ", ".join(
        f"source.{col}" if col in values_columns else f"source.{col}" for col in columns
    )

    merge_query = f"""
    MERGE INTO {quoted_full_name} AS target
    USING {quoted_stage_name} AS source
    ON {pk_conditions}
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(columns)})
        VALUES ({values_part});
    """
    crsr.execute(merge_query)

    crsr.close()


def merge_heap_table_data(conn, table: Table):
    "Merge heap table data from stage into destination table"
    print(f"Merging heap table: {table.table_name}")
    crsr = conn.cursor()

    quoted_stage_name = table.quoted_stage_name()
    quoted_full_name = table.quoted_full_name()

    columns = table.column_list.split(",")

    # The only way to merge heap data is if there's a unique constraint
    if table.uniques:
        print("This heap has uniques")

    # If there are no uniques, then just straight insert all rows
    else:
        insert_heap_query = f"""
        INSERT INTO {quoted_full_name} ({', '.join(columns)})
        SELECT {', '.join(columns)}
        FROM {quoted_stage_name};
        """
        crsr.execute(insert_heap_query)

    crsr.close()


def insert_temporal_history_table_data(conn, table: Table, combined_keys):
    "Insert data from stage for a temporal history table data into destination table"
    print(f"Inserting data for temporal history table table: [{table.table_name}]")
    crsr = conn.cursor()

    quoted_stage_name = table.quoted_stage_name()
    quoted_full_name = table.quoted_full_name()

    columns = table.column_list.split(",")

    # Extract the "parent_column" values from combined_keys
    combined_key_columns = [key["parent_column"] for key in combined_keys]

    # Create a list to store the modified column names
    modified_columns = []

    # Iterate through the columns and modify them if needed
    for column in columns:
        # Check if the column needs modification
        if column in combined_key_columns:
            modified_columns.append(f"New_{column}")
        else:
            modified_columns.append(column)

    # Join the modified column names into a comma-separated string
    modified_column_list = ", ".join(modified_columns)

    # Generate the SQL statement for the insert operation
    insert_query = f"""
    INSERT INTO {quoted_full_name} ({table.column_list})
    SELECT {modified_column_list}
    FROM {quoted_stage_name};
    """

    # Execute the insert query
    crsr.execute(insert_query)

    crsr.close()
