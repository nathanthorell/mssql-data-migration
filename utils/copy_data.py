import pyodbc


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
    src_cnxn = pyodbc.connect(src_conn, autocommit=True)
    src_crsr = src_cnxn.cursor()

    placeholders = ",".join(["?" for _ in column_list.split(",")])

    # Get table data
    get_data_sql = f"SELECT {column_list} FROM [{schema_name}].[{table_name}]"
    src_crsr.execute(get_data_sql)
    records = src_crsr.fetchall()

    src_crsr.close()
    src_cnxn.close()

    dest_cnxn = pyodbc.connect(dest_conn, autocommit=True)
    dest_crsr = dest_cnxn.cursor()

    # Insert records into STAGE table
    if has_identity:
        dest_crsr.execute(f"SET IDENTITY_INSERT [{stage_schema}].[{table_name}] ON")
    dest_crsr.fast_executemany = True
    sql = f"INSERT INTO [{stage_schema}].[{table_name}] ({column_list}) VALUES ({placeholders})"
    dest_crsr.executemany(sql, records)
    if has_identity:
        dest_crsr.execute(f"SET IDENTITY_INSERT [{stage_schema}].[{table_name}] OFF")

    dest_crsr.close()
    dest_cnxn.close()
