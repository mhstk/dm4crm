from abc import ABC

from .general_node import GeneralNode
from typing import Optional, List, Dict


class NonInitialNode(GeneralNode, ABC):
    __slots__ = '_out_port', '_in_port'

    def __init__(self) -> None:
        super().__init__()
        self._in_port: Dict = {}
        self.set_in_port(None, 0)

    def get_in_ports(self):
        return self._in_port

    def get_in_port(self, port_number: int = 0):
        if self._in_port:
            return self._in_port[port_number]
        else:
            return self._in_port

    def set_in_port(self, node: Optional[GeneralNode], port_number: int = 0):
        self._in_port[port_number] = node

