import json
import os
import utils
import pyodbc

# Get directory of current script and construct paths for configs
script_dir = os.path.dirname(__file__)
config_path = os.path.join(script_dir, "config.json")
table_config_path = os.path.join(script_dir, "tables.json")

# Load config files
with open(config_path, "r") as f:
    config = json.load(f)

with open(table_config_path, "r") as f:
    tables = json.load(f)

# Establish DB connections, pass in Connection object to functions instead
src_conn_string = utils.get_conn_string(config=config, type="source")
dest_conn_string = utils.get_conn_string(config=config, type="destination")

src_conn = pyodbc.connect(src_conn_string, autocommit=True)
dest_conn = pyodbc.connect(dest_conn_string, autocommit=True)

# Before starting loop ensure STAGE schema exists at Destination
utils.create_stage_schema(conn=dest_conn)

# Table Schema Constant
SCHEMA = "dbo"
STAGE_SCHEMA = "STAGE"

# Loop through databases, waves, and tables
dest_db = config["destination"]["database"]
db_dict = [d for d in tables["databases"] if d["db_name"] == dest_db]
waves_list = [d["waves"] for d in db_dict][0]
for wave in waves_list:
    print(f"Processing Wave # {wave['wave_num']}...")
    print("#####################################################")
    for table in wave["tables"]:
        utils.create_stage_table(conn=dest_conn, table_name=table, recreate=True)

        # Initialize this for later use during merge loop
        composite_table = False

        # Get table details for PK and columns
        current_pk_list = utils.get_primary_key(
            conn=dest_conn, schema_name=SCHEMA, table_name=table
        )
        has_identity = utils.parse_identity(current_pk_list)
        full_column_list = utils.get_column_list(
            conn=dest_conn, schema_name=SCHEMA, table_name=table
        )
        unique_constraints = utils.get_uniques(
            conn=dest_conn, schema_name=SCHEMA, table_name=table
        )
        column_list_without_pk = utils.get_column_list(
            conn=dest_conn, schema_name=SCHEMA, table_name=table, include_pk=False
        )

        # Stage table setup for PK and FKs
        utils.create_stage_table_pk(
            conn=dest_conn, table_name=table, pk_column_list=current_pk_list
        )
        utils.create_stage_table_newpk(conn=dest_conn, table_name=table)

        current_fks_list = utils.get_foreign_keys(
            conn=dest_conn, schema_name=SCHEMA, table_name=table
        )
        utils.create_stage_table_fks(
            conn=dest_conn,
            stage_schema=STAGE_SCHEMA,
            schema_name=SCHEMA,
            table_name=table,
            foreign_keys=current_fks_list,
        )

        utils.copy_src_table_to_stage(
            src_conn=src_conn,
            dest_conn=dest_conn,
            stage_schema=STAGE_SCHEMA,
            schema_name=SCHEMA,
            table_name=table,
            column_list=full_column_list,
            has_identity=has_identity,
        )

        # Determine the type of table and call correct merge function
        if current_pk_list:
            if has_identity:
                utils.merge_identity_table_data(
                    conn=dest_conn,
                    stage_schema=STAGE_SCHEMA,
                    schema_name=SCHEMA,
                    table_name=table,
                    column_list=column_list_without_pk,
                    uniques=unique_constraints,
                    identity=has_identity,
                )
            else:
                # This will be merged after the FKs are updated
                composite_table = True
        else:
            print("merge_heap_table_data")

        if current_fks_list:
            utils.update_fks_in_stage(
                conn=dest_conn,
                stage_schema=STAGE_SCHEMA,
                table_name=table,
                fks_list=current_fks_list,
            )

        # Now that fks are updated, if table is composite pk, merge
        if composite_table:
            utils.merge_composite_table_data(
                conn=dest_conn,
                stage_schema=STAGE_SCHEMA,
                schema_name=SCHEMA,
                table_name=table,
                column_list=full_column_list,
                pk_columns=current_pk_list,
            )

        print("")

# Close connections
src_conn.close()
dest_conn.close()
