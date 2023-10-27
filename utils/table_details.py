def get_column_list(conn, schema_name, table_name, include_pk=True):
    """get strings of columns from a table"""
    crsr = conn.cursor()

    if include_pk:
        column_list_query = f"""
        DECLARE @column_list NVARCHAR(MAX) = '';
        SELECT @column_list = @column_list + COLUMN_NAME + ','
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}';
        SELECT @column_list AS ColumnList;
        """
    else:
        column_list_query = f"""
        DECLARE @column_list NVARCHAR(MAX) = '';
        SELECT @column_list = @column_list + COLUMN_NAME + ','
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
        AND NOT COLUMNPROPERTY(object_id('{schema_name}.{table_name}'), COLUMN_NAME, 'IsIdentity') = 1;
        SELECT @column_list AS ColumnList;
        """

    crsr.execute(column_list_query)
    result = crsr.fetchone()

    if result.ColumnList is not None:
        column_list = result.ColumnList.rstrip(",")
    else:
        column_list = ""

    crsr.close()
    return column_list


def get_identity(conn, schema_name, table_name):
    "check if table has an auto-incrementing identity PK"
    crsr = conn.cursor()

    identity_query = f"""
        SELECT COLUMN_NAME
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}'
        AND table_name = '{table_name}'
        AND COLUMNPROPERTY(object_id('{schema_name}.{table_name}'), COLUMN_NAME, 'IsIdentity') = 1
    """
    crsr.execute(identity_query)
    has_identity_row = crsr.fetchone()

    if has_identity_row is not None:
        has_identity = has_identity_row[0]
    else:
        has_identity = None

    crsr.close()
    return has_identity


def parse_identity(pk_list):
    "Takes a list of PKs and parses out the Identity column"
    for pk_info in pk_list:
        if pk_info.get("Identity", False):
            return pk_info["PrimaryKeyName"]
    return None  # Return None if no identity primary key is found


def get_column_data_type(conn, schema_name, table_name, column_name):
    "Returns the data type of a tables column"
    crsr = conn.cursor()

    data_type_query = f"""
        SELECT DATA_TYPE + CASE
            WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL
            THEN '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
            ELSE '' END AS DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}'
            AND TABLE_NAME = '{table_name}'
            AND COLUMN_NAME = '{column_name}'
        """
    crsr.execute(data_type_query)
    data_type = crsr.fetchone()[0]

    crsr.close()
    return data_type


def is_pk_entirely_fks(pk_list, fk_list):
    # Extract the column names from the PK and FK lists
    pk_columns = [pk["PrimaryKeyName"] for pk in pk_list]
    fk_columns = [(fk["parent_table"], fk["parent_column"]) for fk in fk_list]

    # Check if all PK columns are in the list of FK columns
    return set(pk_columns).issubset(set(col[1] for col in fk_columns))


def get_temporal_info(conn, schema_name, table_name):
    "Check if the table is Temporal or History"
    crsr = conn.cursor()

    temporal_type_query = f"""
        SELECT CASE
            WHEN temporal_type = 1 THEN 'HISTORY'
            WHEN temporal_type = 2 THEN 'TEMPORAL'
            ELSE 'NON_TEMPORAL'
        END AS temporal_type
        FROM sys.tables
        WHERE object_id = OBJECT_ID('{schema_name}.{table_name}', 'u')
    """
    crsr.execute(temporal_type_query)
    temporal_type = crsr.fetchone()[0]

    if temporal_type == "HISTORY":
        temporal_string = "history_table_id"
    else:
        temporal_string = "object_id"

    temporal_query = f"""
        SELECT
            SCHEMA_NAME(t.schema_id) AS master_schema,
            OBJECT_NAME(t.object_id) AS master_table,
            SCHEMA_NAME(h.schema_id) AS history_schema,
            OBJECT_NAME(h.object_id) AS history_table,
            (
                SELECT c.name
                FROM sys.columns c
                WHERE c.object_id = t.object_id AND c.column_id = p.start_column_id
            ) AS validity_period_start,
            (
                SELECT c.name
                FROM sys.columns c
                WHERE c.object_id = t.object_id AND c.column_id = p.end_column_id
            ) AS validity_period_end
        FROM sys.tables AS t
        LEFT JOIN sys.periods AS p ON t.object_id = p.object_id
        LEFT JOIN sys.tables AS h ON t.history_table_id = h.object_id
        WHERE t.{temporal_string} = OBJECT_ID('{schema_name}.{table_name}', 'u');
        """

    crsr.execute(temporal_query)
    result = crsr.fetchone()

    if result:
        temporal_info = {
            "master_schema": result.master_schema,
            "master_table": result.master_table,
            "temporal_type": temporal_type,
            "history_schema": result.history_schema,
            "history_table": result.history_table,
            "validity_period_start": result.validity_period_start,
            "validity_period_end": result.validity_period_end,
        }
    else:
        temporal_info = {
            "master_schema": schema_name,
            "master_table": table_name,
            "temporal_type": "NON_TEMPORAL",
            "history_schema": None,
            "history_table": None,
            "validity_period_start": None,
            "validity_period_end": None,
        }

    crsr.close()
    return temporal_info


def change_temporal_state(conn, temporal_info, state):
    "State should be either ON or OFF to change SYSTEM_VERSIONING"
    crsr = conn.cursor()

    # Extract information from temporal_info
    master_schema = temporal_info.get("master_schema")
    master_table = temporal_info.get("master_table")
    history_schema = temporal_info.get("history_schema")
    history_table = temporal_info.get("history_table")
    validity_period_start = temporal_info["validity_period_start"]
    validity_period_end = temporal_info["validity_period_end"]

    if state == "ON":
        # This works around the python datetime.datetime only pulling 6 digits of nanoseconds
        # Likely should fix and improve this later on
        for schema, table in [
            (master_schema, master_table),
            (history_schema, history_table),
        ]:
            update_period_end = f"""
            UPDATE [{schema}].[{table}]
            SET {validity_period_end} = (
                SELECT MAX({validity_period_end}) FROM [{schema}].[{table}]
            );
            """
            crsr.execute(update_period_end)

        # Before enabling SYSTEM_VERSIONING the PERIOD needs to be added back
        add_period_query = f"""
        ALTER TABLE [{master_schema}].[{master_table}]
        ADD PERIOD FOR SYSTEM_TIME ({validity_period_start}, {validity_period_end});
        """
        crsr.execute(add_period_query)

        temporal_query = f"""
        ALTER TABLE [{master_schema}].[{master_table}]
        SET (
            SYSTEM_VERSIONING = ON (
                HISTORY_TABLE = {history_schema}.{history_table},
                DATA_CONSISTENCY_CHECK = ON
            )
        );
        """
        crsr.execute(temporal_query)

    elif state == "OFF":
        temporal_query = f"""
        ALTER TABLE [{master_schema}].[{master_table}]
        SET (SYSTEM_VERSIONING = OFF);
        """
        crsr.execute(temporal_query)

        drop_period_query = f"""
        ALTER TABLE [{master_schema}].[{master_table}]
        DROP PERIOD FOR SYSTEM_TIME;
        """
        crsr.execute(drop_period_query)

    print(f"Temporal Table [{master_table}] now has it's SYSTEM_VERSIONING = {state}")
    crsr.close()
