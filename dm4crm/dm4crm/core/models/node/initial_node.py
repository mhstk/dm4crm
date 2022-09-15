from abc import ABC

from .general_node import GeneralNode


class InitialNode(GeneralNode, ABC):
    __slots__ = '_out_port'


