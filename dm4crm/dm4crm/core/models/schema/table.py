from typing import Optional, List

from .schema import Schema


class Table:
    def __init__(self):
        self.schema: Optional[Schema] = None
        self.rows: List = []