from utils.table_details import get_column_data_type
from utils.Table import Table


def create_key_stage(conn, table: Table):
    "create KeyStage table if it doesnt exist"
    crsr = conn.cursor()

    identity_data_type = get_column_data_type(
        conn=conn, table=table, column_name=table.identity
    )

    # Construct column names and data types for the staging table
    new_identity_column = f"New_{table.identity}"
    source_identity_column = f"Source_{table.identity}"

    # Clean up the staging table if needed
    cleanup_staging_table_sql = f"""
        DROP TABLE IF EXISTS [{table.stage_schema}].[KeyStage];
    """
    crsr.execute(cleanup_staging_table_sql)

    # Create a permanent staging table if it doesn't already exist
    create_staging_table_sql = f"""
        CREATE TABLE [{table.stage_schema}].[KeyStage] (
            [{new_identity_column}] {identity_data_type},
            [{source_identity_column}] {identity_data_type}
        )
    """
    crsr.execute(create_staging_table_sql)

    crsr.close()

    return new_identity_column, source_identity_column


def update_new_pk_in_stage(conn, table: Table, key_arrays):
    "Update the New_ column in the STAGE schema"
    crsr = conn.cursor()

    # Parse the key_array
    column_name = key_arrays["column_name"]
    inserted_identity_values = key_arrays["inserted_identity_values"]
    source_identity_values = key_arrays["source_identity_values"]

    # Construct and execute dynamic SQL statements to update New_ column
    for inserted_id, source_id in zip(inserted_identity_values, source_identity_values):
        update_sql = f"""
        UPDATE [{table.stage_schema}].[{table.table_name}]
        SET New_{column_name} = ?
        WHERE {column_name} = ?
        """
        crsr.execute(update_sql, inserted_id, source_id)

    crsr.close()


def update_fks_in_stage(conn, table: Table):
    """"""
    crsr = conn.cursor()

    quoted_stage_name = table.quoted_stage_name()

    for fk in table.fk_column_list:
        update_query = f"""
            UPDATE stage
            SET stage.New_{fk['parent_column']} =
            COALESCE(parent.New_{fk['referenced_column']}, parent.{fk['referenced_column']})
            FROM {quoted_stage_name} stage
            INNER JOIN [{table.stage_schema}].[{fk['referenced_table']}] parent
            ON stage.{fk['parent_column']} = parent.{fk['referenced_column']}
        """
        crsr.execute(update_query)

        print(f"Updated Foreign Key [{fk['name']}]")

    crsr.close()


def update_pk_columns_in_unique_stage(conn, table: Table):
    "Unique tables have New_ columns that should just match the values of their base columns"
    crsr = conn.cursor()

    quoted_stage_name = table.quoted_stage_name()

    for pk in table.pk_column_list:
        update_query = f"""
            UPDATE {quoted_stage_name}
            SET New_{pk["PrimaryKeyName"]} = {pk["PrimaryKeyName"]}
        """
        crsr.execute(update_query)

        print(f"Updated Unique Table {quoted_stage_name} Key columns: [{pk['PrimaryKeyName']}]")

    crsr.close()


def update_temporal_history_stage_keys(conn, table: Table, key_list):
    ""
    crsr = conn.cursor()

    quoted_stage_name = table.quoted_stage_name()

    for key in key_list:
        update_query = f"""
            UPDATE {quoted_stage_name}
            SET New_{key['parent_column']} =
            COALESCE(parent.New_{key['referenced_column']}, parent.{key['referenced_column']})
            FROM {quoted_stage_name} stage
            INNER JOIN [{table.stage_schema}].[{key['referenced_table']}] parent
            ON stage.{key['parent_column']} = parent.{key['referenced_column']}
        """
        crsr.execute(update_query)

    # There may still be records in History that no longer have a corresponding parent Id
    # because the parent rows might have been removed.  In order to keep the history rows and not
    # point them at incorrect parents, we will replace them with their negative equivalents.
    # NOTE: This is a hack, but it's the best we can do.
    for key in key_list:
        update_query = f"""
            UPDATE {quoted_stage_name}
            SET New_{key['parent_column']} = {key['parent_column']} * -1
            WHERE New_{key['parent_column']} IS NULL
        """
        crsr.execute(update_query)

    crsr.close()
