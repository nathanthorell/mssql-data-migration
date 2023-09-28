import pyodbc


def get_primary_key(conn, table_name):
    "query the table and return an object of the primary key"
    cnxn = pyodbc.connect(conn, autocommit=True)
    crsr = cnxn.cursor()

    pk_query = f"""
    SELECT c.name AS PrimaryKeyName, c.column_id AS ColumnId,
        TYPE_NAME(c.system_type_id) AS ColumnType, c.is_identity AS [Identity]
    FROM sys.key_constraints kc
    INNER JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id  and kc.unique_index_id = ic.index_id
    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE kc.type = 'PK' AND OBJECT_NAME(kc.parent_object_id) = '{table_name}'
    """

    crsr.execute(pk_query)
    result = crsr.fetchall()
    columns = [column[0] for column in crsr.description]

    crsr.close()

    result_list = [dict(zip(columns, row)) for row in result]
    return result_list


def get_foreign_keys(conn, schema_name, table_name):
    cnxn = pyodbc.connect(conn, autocommit=True)
    crsr = cnxn.cursor()

    foreign_keys_query = f"""
    SELECT
        FK.name AS foreign_key_name,
        OBJECT_NAME(FKC.parent_object_id) AS parent_table,
        C.name AS parent_column,
        OBJECT_NAME(FKC.referenced_object_id) AS referenced_table,
        CR.name AS referenced_column
    FROM sys.foreign_keys AS FK
    JOIN sys.foreign_key_columns AS FKC ON FK.object_id = FKC.constraint_object_id
    JOIN sys.columns AS C ON FKC.parent_column_id = C.column_id
        AND FKC.parent_object_id = C.object_id
    JOIN sys.columns AS CR ON FKC.referenced_column_id = CR.column_id
        AND FKC.referenced_object_id = CR.object_id
    WHERE OBJECT_SCHEMA_NAME(FK.parent_object_id) = '{schema_name}'
        AND OBJECT_NAME(FK.parent_object_id) = '{table_name}'
    ORDER BY foreign_key_name;
    """

    crsr.execute(foreign_keys_query)

    foreign_keys = []
    for row in crsr:
        foreign_key = {
            "name": row.foreign_key_name,
            "parent_table": row.parent_table,
            "parent_column": row.parent_column,
            "referenced_table": row.referenced_table,
            "referenced_column": row.referenced_column,
        }
        foreign_keys.append(foreign_key)

    crsr.close()
    cnxn.close()

    return foreign_keys


def get_uniques(conn, schema_name, table_name):
    "Gets any UNIQUE constraints from the table"
    cnxn = pyodbc.connect(conn, autocommit=True)
    crsr = cnxn.cursor()

    unique_constraints_query = f"""
    SELECT DISTINCT i.name AS constraint_name, c.name AS column_name
    FROM sys.indexes AS i
    JOIN sys.index_columns AS ic ON i.object_id = ic.object_id
            AND i.index_id = ic.index_id
    JOIN sys.columns AS c ON ic.object_id = c.object_id
            AND ic.column_id = c.column_id
    WHERE i.is_unique = 1
        AND OBJECT_SCHEMA_NAME(i.object_id) = '{schema_name}'
        AND OBJECT_NAME(i.object_id) = '{table_name}'
    ORDER BY constraint_name, column_name;
    """
    crsr.execute(unique_constraints_query)

    unique_constraints = {}
    for row in crsr:
        constraint_name = row.constraint_name
        column_name = row.column_name

        # Exclude constraints that match the primary key
        primary_key_columns = get_primary_key(conn, schema_name, table_name)
        if column_name not in primary_key_columns:
            if constraint_name not in unique_constraints:
                unique_constraints[constraint_name] = []
            unique_constraints[constraint_name].append(column_name)

    crsr.close()
    cnxn.close()

    return unique_constraints