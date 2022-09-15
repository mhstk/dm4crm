from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
from ...node_wrapper import NodeWrapper


class Converter(ABC):
    __slots__ = '_node_wrapper'

    def __init__(self) -> None:
        self._node_wrapper: Optional[NodeWrapper] = None

    def get_node_wrapper(self):
        return self._node_wrapper

    def set_node_wrapper(self, node_wrapper: NodeWrapper) -> Converter:
        self._node_wrapper = node_wrapper
        return self

    @abstractmethod
    def convert(self):
        pass
