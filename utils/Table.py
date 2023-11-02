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
    pk_column_list: list = None
    fk_column_list: list = None
    column_list: list = None
    uniques: dict = None

    def update_type(self, type: TableType):
        self.type = type

    def quoted_stage_name(self):
        return f"[{self.stage_schema}].[{self.table_name}]"

    def quoted_full_name(self):
        return f"[{self.schema_name}].[{self.table_name}]"

    def is_pk_entirely_fks(self):
        # Extract the column names from the PK and FK lists
        pk_columns = [pk["PrimaryKeyName"] for pk in self.pk_column_list]
        fk_columns = [(fk["parent_table"], fk["parent_column"]) for fk in self.fk_column_list]

        # Check if all PK columns are in the list of FK columns
        return set(pk_columns).issubset(set(col[1] for col in fk_columns))

    def parse_identity(self):
        "Takes a list of PKs and parses out the Identity column"
        for pk_info in self.pk_column_list:
            if pk_info.get("Identity", False):
                self.identity = pk_info["PrimaryKeyName"]
