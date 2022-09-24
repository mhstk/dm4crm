from typing import Optional, List

from ..non_initial_node import NonInitialNode


class SeparateTargetColumnNode(NonInitialNode):
    __slots__ = 'target_column',

    def __init__(self, target_column: Optional[List] = None):
        super(SeparateTargetColumnNode, self).__init__()
        self.set_out_port(None, 0)
        self.set_out_port(None, 1)
        self.target_column = target_column if target_column else []

    def set_attribute(self, *args, **kwargs):
        self.target_column = kwargs.get("target_column", self.target_column)
