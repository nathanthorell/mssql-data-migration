from dataclasses import dataclass
from typing import Any
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
    identity: Any = None

    def update_type(self, type: TableType):
        self.type = type

    def quoted_stage_name(self):
        return f"[{self.stage_schema}].[{self.table_name}]"

    def quoted_full_name(self):
        return f"[{self.schema_name}].[{self.table_name}]"
