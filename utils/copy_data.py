from utils.update_keys import create_key_stage, update_new_pk_in_stage


def copy_src_table_to_stage(
    src_conn,
    dest_conn,
    stage_schema,
    schema_name,
    table_name,
    column_list,
    has_identity=True,
):
    "copy a tables data from src_conn to dest_conn in stage schema"
    src_crsr = src_conn.cursor()

    print(f"Starting source to stage table copy of [{table_name}]...")

    placeholders = ",".join(["?" for _ in column_list.split(",")])

    # Get table data
    get_data_sql = f"SELECT {column_list} FROM [{schema_name}].[{table_name}]"
    src_crsr.execute(get_data_sql)
    records = src_crsr.fetchall()

    src_crsr.close()

    dest_crsr = dest_conn.cursor()

    # Insert records into STAGE table
    if has_identity:
        dest_crsr.execute(f"SET IDENTITY_INSERT [{stage_schema}].[{table_name}] ON")
    dest_crsr.fast_executemany = True
    sql = f"INSERT INTO [{stage_schema}].[{table_name}] ({column_list}) VALUES ({placeholders})"
    dest_crsr.executemany(sql, records)
    if has_identity:
        dest_crsr.execute(f"SET IDENTITY_INSERT [{stage_schema}].[{table_name}] OFF")

    dest_crsr.close()


def merge_identity_table_data(
    conn, stage_schema, schema_name, table_name, column_list, uniques, identity
):
    "take data from stage and insert it into destination tables returning PK values to stage"
    crsr = conn.cursor()

    columns = column_list.split(",")

    new_identity_column, source_identity_column = create_key_stage(
        conn=conn,
        stage_schema=stage_schema,
        schema_name=schema_name,
        table_name=table_name,
        identity=identity,
    )

    # build the WHEN condition based on uniques
    if uniques:
        when_conditions = []
        for constraint_name, uq_columns in uniques.items():
            # The PK is technically a UNIQUE constraint, but we need to ignore it
            if not all(column in identity for column in uq_columns):
                # Create a list of conditions for each column in the unique constraint
                column_conditions = []
                for col in uq_columns:
                    # Check that the source column is not NULL
                    column_conditions.append(f"source.[{col}] IS NOT NULL")

                combined_condition = " AND ".join(column_conditions)

                # Include the duplicate checking logic using NOT EXISTS
                duplicate_check = f"""
                NOT EXISTS (SELECT 1
                FROM [{schema_name}].[{table_name}] AS existing
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
    MERGE INTO [{schema_name}].[{table_name}] AS target
    USING [{stage_schema}].[{table_name}] AS source
    ON 1 = 0  -- Ensures the INSERT part of the MERGE is executed for all rows
    {when_condition} THEN
        INSERT ({', '.join(columns)})
        VALUES ({', '.join('source.' + col for col in columns)})
    OUTPUT inserted.{identity} AS [{new_identity_column}], source.{identity} AS [{source_identity_column}]
    INTO [{stage_schema}].[KeyStage];
    """
    crsr.execute(merge_query)

    # Retrieve data from the StagingTable
    select_staging_data_sql = f"""
        SELECT [{new_identity_column}], [{source_identity_column}]
        FROM [{stage_schema}].[KeyStage]
    """
    crsr.execute(select_staging_data_sql)

    # Fetch the results into Python variables
    key_arrays = {
        "column_name": identity,
        "inserted_identity_values": [],
        "source_identity_values": [],
    }

    for row in crsr:
        key_arrays["inserted_identity_values"].append(row[0])
        key_arrays["source_identity_values"].append(row[1])

    update_new_pk_in_stage(
        conn=conn,
        stage_schema=stage_schema,
        table_name=table_name,
        key_arrays=key_arrays,
    )

    # Identify any remaining null PKs that didnt get updated above
    # and assume these are duplicates from a UNIQUE constraint so set the New_
    # PK equal to the original PK
    # TODO: There's probably a better way to do this, might fix later
    unique_null_pks_sql = f"""
    UPDATE [{stage_schema}].[{table_name}]
    SET New_{identity} = {identity}
    WHERE New_{identity} IS NULL
    """
    crsr.execute(unique_null_pks_sql)

    crsr.close()


def merge_composite_table_data(
    conn, stage_schema, schema_name, table_name, column_list, pk_columns
):
    "take composite pk data from stage and insert it into destination table"
    print(f"Merging composite table: {table_name}")
    crsr = conn.cursor()

    columns = column_list.split(",")

    # Construct the list of PK columns in the format:
    # "source.New_column1 = target.column1 AND source.New_column2 = target.column2"
    pk_conditions = []
    values_columns = []
    for pk_col in pk_columns:
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
    MERGE INTO [{schema_name}].[{table_name}] AS target
    USING [{stage_schema}].[{table_name}] AS source
    ON {pk_conditions}
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(columns)})
        VALUES ({values_part});
    """
    crsr.execute(merge_query)

    crsr.close()
