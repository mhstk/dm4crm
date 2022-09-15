from typing import List, Optional, Dict

from .node.general_node import GeneralNode
from .schema.schema import Schema


class NodeWrapper:
    def __init__(self, node: GeneralNode) -> None:
        self._node: GeneralNode = node
        self._schema: Optional[Schema] = None
        self._in_idents: Dict = {}
        self._out_idents: Dict = {}

    def get_node(self):
        return self._node

    def set_node(self, node: GeneralNode):
        self._node = node

    def get_in_idents(self):
        return self._in_idents

    def set_in_ident(self, index: int, ident: str):
        self._in_idents[index] = ident

    def get_out_idents(self):
        return self._out_idents

    def set_out_ident(self, index: int, ident: str):
        self._out_idents[index] = ident

    def set_schema(self, schema: Schema):
        self._schema = schema

    def get_schema(self):
        return self._schema
