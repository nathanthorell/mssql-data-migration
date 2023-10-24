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

    return data_type


def is_pk_entirely_fks(pk_list, fk_list):
    # Extract the column names from the PK and FK lists
    pk_columns = [pk['PrimaryKeyName'] for pk in pk_list]
    fk_columns = [(fk['parent_table'], fk['parent_column']) for fk in fk_list]

    # Check if all PK columns are in the list of FK columns
    return set(pk_columns).issubset(set(col[1] for col in fk_columns))
