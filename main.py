import json
import os
import utils

# Get directory of current script and construct paths for configs
script_dir = os.path.dirname(__file__)
config_path = os.path.join(script_dir, "config.json")
table_config_path = os.path.join(script_dir, "tables.json")

# Load config files
with open(config_path, "r") as f:
    config = json.load(f)

with open(table_config_path, "r") as f:
    tables = json.load(f)

src_conn = utils.get_conn(config=config, type="source")
dest_conn = utils.get_conn(config=config, type="destination")

# Before starting loop ensure STAGE schema exists at Destination
utils.create_stage_schema(conn=dest_conn)

# Loop through databases, waves, and tables
dest_db = config["destination"]["database"]
db_dict = [d for d in tables["databases"] if d["db_name"] == dest_db]
waves_list = [d["waves"] for d in db_dict][0]
for wave in waves_list:
    print(f"Processing Wave # {wave['wave_num']}...")
    print("#####################################################")
    for table in wave["tables"]:
        utils.create_stage_table(conn=dest_conn, table_name=table, recreate=True)

        # Stage table setup for Primary Keys
        current_pk_list = utils.get_primary_key(conn=dest_conn, table_name=table)

        utils.create_stage_table_pk(
            conn=dest_conn, table_name=table, pk_column_list=current_pk_list
        )
        utils.create_stage_table_newpk(conn=dest_conn, table_name=table)

        # Stage table setup for Foreign Keys
        current_fks_list = utils.get_foreign_keys(
            conn=dest_conn, schema_name="dbo", table_name=table
        )
        utils.create_stage_table_fks(
            conn=dest_conn,
            source_schema="dbo",
            table_name=table,
            foreign_keys=current_fks_list,
        )

        print(f"Starting data copy of [{table}]...")
        print("")
