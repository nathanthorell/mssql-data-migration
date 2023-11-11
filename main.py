import json
import os
import utils
import pyodbc
from time import gmtime, strftime, time
from datetime import datetime

# Script Timer
start_time = time()

# Constants
SCHEMA = "dbo"
STAGE_SCHEMA = "STAGE"

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

# Loop through databases, waves, and tables
dest_db = config["destination"]["database"]
db_dict = [d for d in tables["databases"] if d["db_name"] == dest_db]
waves_list = [d["waves"] for d in db_dict][0]
for wave in waves_list:
    print(f"Processing Wave # {wave['wave_num']}...")
    print(strftime("%Y-%m-%d %H:%M:%S", gmtime()))
    print("#####################################################")
    for table in wave["tables"]:
        current_table = utils.Table(SCHEMA, STAGE_SCHEMA, table)

        utils.create_stage_table(conn=dest_conn, table=current_table, recreate=True)

        # Gather the table details
        current_table.pk_column_list = utils.get_primary_key(
            conn=dest_conn, table=current_table
        )
        current_table.fk_column_list = utils.get_foreign_keys(
            conn=dest_conn, table=current_table
        )
        current_table.uniques = utils.get_uniques(conn=dest_conn, table=current_table)
        current_table.column_list = utils.get_column_list(
            conn=dest_conn, table=current_table
        )
        current_table.column_list_without_identity = utils.get_column_list(
            conn=dest_conn, table=current_table, include_identity=False
        )
        current_table.get_identity(conn=dest_conn)
        current_table.column_list_with_new_keys = utils.columns_with_new_keys(
            table=current_table, include_identity=True
        )
        current_table.column_list_new_keys_without_identity = (
            utils.columns_with_new_keys(table=current_table, include_identity=False)
        )
        current_table.get_clustered_on(conn=dest_conn)
        is_pk_composite = current_table.is_pk_entirely_fks()
        temporal_info = utils.get_temporal_info(conn=dest_conn, table=current_table)

        if temporal_info:
            temporal_type = temporal_info["temporal_type"]

        # Check for table type and update current Table variables

        if current_table.identity:
            current_table.update_type("IDENTITY")
        elif is_pk_composite:
            current_table.update_type("COMPOSITE")
        elif current_table.uniques:
            current_table.update_type("UNIQUE")
        else:
            current_table.update_type("HEAP")

        # Stage table setup for PK and FKs
        if current_table.pk_column_list:
            utils.create_stage_table_pk(conn=dest_conn, table=current_table)
            utils.create_stage_table_newpk(conn=dest_conn, table=current_table)

        if current_table.fk_column_list:
            utils.create_stage_table_fks(conn=dest_conn, table=current_table)

        # Handle rare scenario where the identity column is not part of the PK
        if current_table.identity and current_table.identity not in [
            column["PrimaryKeyName"] for column in current_table.pk_column_list
        ]:
            utils.create_stage_table_identity(conn=dest_conn, table=current_table)

        # If table is a Temporal History table, add keys in Stage
        if temporal_type == "HISTORY":
            combined_keys = utils.get_temporal_combined_keys(
                conn=dest_conn, stage_schema=STAGE_SCHEMA, temporal_info=temporal_info
            )
            utils.create_stage_temporal_history_keys(
                conn=dest_conn,
                table=current_table,
                temporal_info=temporal_info,
                combined_keys=combined_keys,
            )

        # Disable SYSTEM_VERSIONING in order to Process Temporal Tables
        if temporal_type in ["TEMPORAL", "HISTORY"]:
            utils.change_temporal_state(
                conn=dest_conn, temporal_info=temporal_info, state="OFF"
            )

        utils.copy_src_table_to_stage(
            src_conn=src_conn,
            dest_conn=dest_conn,
            table=current_table,
        )

        # Before the table merge update any FKs in Stage
        if current_table.fk_column_list:
            utils.update_fks_in_stage(conn=dest_conn, table=current_table)

        # If temporal_type = HISTORY, treat master_table's PK as a FK in History to be updated accordingly
        # This must be done before re-enabling SYSTEM_VERSIONING
        if temporal_type == "HISTORY":
            utils.update_temporal_history_stage_keys(
                conn=dest_conn, table=current_table, key_list=combined_keys
            )

        # Call correct merge function based on TableType
        match current_table.type:
            case "IDENTITY":
                utils.merge_identity_table_data(conn=dest_conn, table=current_table)
            case "UNIQUE":
                utils.update_pk_columns_in_unique_stage(
                    conn=dest_conn, table=current_table
                )
                utils.merge_unique_table_data(conn=dest_conn, table=current_table)
            case "COMPOSITE":
                utils.merge_composite_table_data(conn=dest_conn, table=current_table)
            case "HEAP":
                if temporal_type == "HISTORY":
                    utils.insert_temporal_history_table_data(
                        conn=dest_conn,
                        table=current_table,
                        combined_keys=combined_keys,
                    )
                else:
                    utils.merge_heap_table_data(conn=dest_conn, table=current_table)

        # Re-Enable SYSTEM_VERSIONING after MERGE is finished
        if temporal_type in ["TEMPORAL", "HISTORY"]:
            utils.change_temporal_state(
                conn=dest_conn, temporal_info=temporal_info, state="ON"
            )

        print("")

# Close connections
src_conn.close()
dest_conn.close()

# End the timer
end_time = time()
runtime = end_time - start_time
formatted_runtime = datetime.utcfromtimestamp(runtime).strftime("%H:%M:%S")
print(f"Script executed in {formatted_runtime}")
