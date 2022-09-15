from typing import List

from .column import Column


class Schema:
    def __init__(self):
        self.columns: List[Column] = []

    def __str__(self):
        out_str = ''
        for column in self.columns:
            out_str += f"{column.name:<20} {column.type}" + "\n"
        return out_str
