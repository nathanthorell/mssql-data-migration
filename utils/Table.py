from dataclasses import dataclass
from enum import Enum


class TableType(Enum):
    IDENTITY = "Identity"
    UNIQUE = "Unique"
    COMPOSITE = "Composite"
    HEAP = "Heap"


@dataclass
class Table:
    schema_name: str
    stage_schema: str
    table_name: str
    type: TableType = None
    identity: str = None
    clustered: list = None
    pk_column_list: list = None
    fk_column_list: list = None
    column_list: list = None
    column_list_without_identity: list = None
    column_list_with_new_keys: list = None
    column_list_new_keys_without_identity: list = None
    uniques: dict = None
    combined_keys: list = None
    temporal_info: dict = None

    def update_type(self, type: TableType):
        self.type = type

    def quoted_stage_name(self):
        return f"[{self.stage_schema}].[{self.table_name}]"

    def quoted_full_name(self):
        return f"[{self.schema_name}].[{self.table_name}]"

    def is_pk_entirely_fks(self):
        # Extract the column names from the PK and FK lists
        pk_columns = [pk["PrimaryKeyName"] for pk in self.pk_column_list]
        fk_columns = [
            (fk["parent_table"], fk["parent_column"]) for fk in self.fk_column_list
        ]

        # Check if all PK columns are in the list of FK columns
        if pk_columns:
            return set(pk_columns).issubset(set(col[1] for col in fk_columns))
        else:
            return None

    def get_identity(self, conn):
        "check if table has an auto-incrementing identity column"
        crsr = conn.cursor()

        identity_query = f"""
            SELECT COLUMN_NAME
            FROM information_schema.columns
            WHERE table_schema = '{self.schema_name}'
            AND table_name = '{self.table_name}'
            AND COLUMNPROPERTY(object_id('{self.schema_name}.{self.table_name}'), COLUMN_NAME, 'IsIdentity') = 1
        """
        crsr.execute(identity_query)
        has_identity_row = crsr.fetchone()

        if has_identity_row is not None:
            self.identity = has_identity_row[0]

        crsr.close()

    def get_clustered_on(self, conn):
        "Get the columns the table is clustered on"
        crsr = conn.cursor()

        clustered_query = f"""
        SELECT col.name AS clustered_column_name
        FROM sys.indexes AS idx
        INNER JOIN sys.tables AS tbl ON idx.object_id = tbl.object_id
        INNER JOIN sys.index_columns AS ic ON idx.object_id = ic.object_id AND idx.index_id = ic.index_id
        INNER JOIN sys.columns AS col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
        WHERE idx.index_id = 1   -- Clustered index has index_id 1
        AND OBJECT_SCHEMA_NAME(tbl.object_id) = '{self.schema_name}'
        AND tbl.name = '{self.table_name}'
        """
        crsr.execute(clustered_query)
        result = crsr.fetchall()
        result_list = [row[0] for row in result]

        self.clustered = result_list

        crsr.close()
