from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from .non_initial_node import NonInitialNode


class GeneralNode(ABC):
    __slots__ = '_out_port'

    def __init__(self) -> None:
        self._out_port: Dict = {}
        self.set_out_port(None, 0)

    def get_out_ports(self):
        return self._out_port

    def get_out_port(self, port_number: int = 0):
        if self._out_port:
            return self._out_port[port_number]
        return self._out_port

    @abstractmethod
    def set_attribute(self, *args, **kwargs):
        pass

    def set_out_port(self, node: Optional[GeneralNode], port_number: int = 0):
        self._out_port[port_number] = node

    def connect_to(self, dest_node: NonInitialNode, from_port: int = 0, dest_port: int = 0) -> bool:
        # noinspection PyBroadException
        try:
            self.set_out_port(dest_node, from_port)
            dest_node.set_in_port(self, dest_port)
            return True

        except Exception:
            return False

    def full_remove_connected_nodes(self, dest_node: NonInitialNode):
        for port, node in self._out_port.items():
            if node == dest_node:
                self.set_out_port(None, port)
        for port, node in dest_node.get_in_ports().items():
            if node == self:
                dest_node.set_in_port(None, port)

    def remove_connected_node(self, dest_node: NonInitialNode, from_port: int = 0, dest_port: int = 0) -> bool:
        # noinspection PyBroadException
        try:
            if self.get_out_ports()[from_port] == dest_node and dest_node.get_in_ports()[dest_port] == self:
                self.set_out_port(None, from_port)
                dest_node.set_in_port(None, dest_port)
                return True
        except Exception:
            pass
        return False
