import json
import os
import utils
import pyodbc
from time import gmtime, strftime
from enum import Enum

# Constants
SCHEMA = "dbo"
STAGE_SCHEMA = "STAGE"


class TableType(Enum):
    IDENTITY = "Identity"
    UNIQUE = "Unique"
    COMPOSITE = "Composite"
    HEAP = "Heap"


class Table:
    def __init__(
        self, schema_name, stage_schema, table_name, type: TableType = None
    ) -> None:
        self.schema_name = schema_name
        self.stage_schema = stage_schema
        self.table_name = table_name
        self.type = type

    def update_type(self, type: TableType):
        self.type = type

    def __str__(self) -> str:
        if self.type:
            return f"Table: [{self.schema_name}].[{self.table_name}] Type: {self.type}"
        else:
            return f"Table: [{self.schema_name}].[{self.table_name}]"


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
        current_table = Table(SCHEMA, STAGE_SCHEMA, table)

        utils.create_stage_table(conn=dest_conn, table_name=table, recreate=True)

        # Gather the table details
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
        current_fks_list = utils.get_foreign_keys(
            conn=dest_conn, schema_name=SCHEMA, table_name=table
        )
        is_pk_composite = utils.is_pk_entirely_fks(
            pk_list=current_pk_list, fk_list=current_fks_list
        )
        temporal_info = utils.get_temporal_info(
            conn=dest_conn, schema_name=SCHEMA, table_name=table
        )

        if temporal_info:
            temporal_type = temporal_info["temporal_type"]

        # Check for table type and update current Table variables
        if current_pk_list:
            if has_identity:
                current_table.update_type("IDENTITY")
            elif is_pk_composite:
                current_table.update_type("COMPOSITE")
            else:
                current_table.update_type("UNIQUE")
        else:
            current_table.update_type("HEAP")

        # Stage table setup for PK and FKs
        if current_pk_list:
            utils.create_stage_table_pk(
                conn=dest_conn, table_name=table, pk_column_list=current_pk_list
            )
            utils.create_stage_table_newpk(conn=dest_conn, table_name=table)

        if current_fks_list:
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

        # Disable SYSTEM_VERSIONING in order to MERGE
        if temporal_type in ["TEMPORAL", "HISTORY"]:
            utils.change_temporal_state(
                conn=dest_conn, temporal_info=temporal_info, state="OFF"
            )

        # Call correct merge function based on TableType
        if current_table.type == "IDENTITY":
            utils.merge_identity_table_data(
                conn=dest_conn,
                stage_schema=STAGE_SCHEMA,
                schema_name=SCHEMA,
                table_name=table,
                column_list=column_list_without_pk,
                uniques=unique_constraints,
                identity=has_identity,
            )
        elif current_table.type == "UNIQUE":
            utils.merge_unique_table_data(
                conn=dest_conn,
                stage_schema=STAGE_SCHEMA,
                schema_name=SCHEMA,
                table_name=table,
                column_list=full_column_list,
                pk_columns=current_pk_list,
            )
        elif current_table.type == "HEAP":
            utils.merge_heap_table_data(
                conn=dest_conn,
                stage_schema=STAGE_SCHEMA,
                schema_name=SCHEMA,
                table_name=table,
                column_list=full_column_list,
                uniques=unique_constraints,
                temporal=temporal_info,
            )

        # Re-Enable SYSTEM_VERSIONING after MERGE is finished
        if temporal_type in ["TEMPORAL", "HISTORY"]:
            utils.change_temporal_state(
                conn=dest_conn, temporal_info=temporal_info, state="ON"
            )

        # After the table merge is complete update any FKs
        if current_fks_list:
            utils.update_fks_in_stage(
                conn=dest_conn,
                stage_schema=STAGE_SCHEMA,
                table_name=table,
                fks_list=current_fks_list,
            )

        # Now that FKs are updated, if TableType is Composite, then merge it's data
        if current_table.type == "COMPOSITE":
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
