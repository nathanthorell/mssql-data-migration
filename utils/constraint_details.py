from utils.Table import Table


def get_primary_key(conn, table: Table):
    "query the table and return an object of the primary key"
    crsr = conn.cursor()

    pk_query = f"""
    SELECT c.name AS PrimaryKeyName, c.column_id AS ColumnId,
        TYPE_NAME(c.system_type_id) AS ColumnType, c.is_identity AS [Identity]
    FROM sys.key_constraints kc
    INNER JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id  and kc.unique_index_id = ic.index_id
    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE kc.type = 'PK' AND OBJECT_SCHEMA_NAME(kc.parent_object_id) = '{table.schema_name}'
    AND OBJECT_NAME(kc.parent_object_id) = '{table.table_name}'
    """

    crsr.execute(pk_query)
    result = crsr.fetchall()
    columns = [column[0] for column in crsr.description]

    crsr.close()

    result_list = [dict(zip(columns, row)) for row in result]
    return result_list


def get_foreign_keys(conn, table: Table):
    crsr = conn.cursor()

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
    WHERE OBJECT_SCHEMA_NAME(FK.parent_object_id) = '{table.schema_name}'
        AND OBJECT_NAME(FK.parent_object_id) = '{table.table_name}'
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

    return foreign_keys


def get_uniques(conn, table: Table):
    "Gets any UNIQUE constraints from the table"
    crsr = conn.cursor()

    unique_constraints_query = f"""
    SELECT DISTINCT i.name AS constraint_name, c.name AS column_name
    FROM sys.indexes AS i
    JOIN sys.index_columns AS ic ON i.object_id = ic.object_id
            AND i.index_id = ic.index_id
    JOIN sys.columns AS c ON ic.object_id = c.object_id
            AND ic.column_id = c.column_id
    WHERE i.is_unique = 1
        AND OBJECT_SCHEMA_NAME(i.object_id) = '{table.schema_name}'
        AND OBJECT_NAME(i.object_id) = '{table.table_name}'
    ORDER BY constraint_name, column_name;
    """
    crsr.execute(unique_constraints_query)

    unique_constraints = {}
    for row in crsr:
        constraint_name = row.constraint_name
        column_name = row.column_name

        # Exclude constraints that match the primary key
        primary_key_columns = get_primary_key(conn=conn, table=table)
        if column_name not in primary_key_columns:
            if constraint_name not in unique_constraints:
                unique_constraints[constraint_name] = []
            unique_constraints[constraint_name].append(column_name)

    crsr.close()

    return unique_constraints


def get_temporal_combined_keys(conn, stage_schema, temporal_info):
    temporal_master_schema = temporal_info["master_schema"]
    temporal_master_table = temporal_info["master_table"]
    temporal_history_table = temporal_info["history_table"]

    master_table = Table(
        schema_name=temporal_master_schema,
        stage_schema=stage_schema,
        table_name=temporal_master_table,
    )

    temporal_pk = get_primary_key(conn=conn, table=master_table)
    temporal_fks = get_foreign_keys(conn=conn, table=master_table)

    # Add primary and foreign keys to the list of combined_keys
    combined_keys = []
    for pk in temporal_pk:
        combined_keys.append(
            {
                "origin_key_type": "PrimaryKey",
                "parent_table": temporal_history_table,
                "parent_column": pk["PrimaryKeyName"],
                "referenced_table": temporal_master_table,
                "referenced_column": pk["PrimaryKeyName"],
            }
        )
    for fk in temporal_fks:
        combined_keys.append(
            {
                "origin_key_type": "ForeignKey",
                "parent_table": fk["parent_column"],
                "parent_column": fk["parent_column"],
                "referenced_table": fk["referenced_table"],
                "referenced_column": fk["referenced_column"],
            }
        )

    return combined_keys
