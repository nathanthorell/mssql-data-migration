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
