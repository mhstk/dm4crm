from typing import Optional, List

from ..non_initial_node import NonInitialNode


class SplitColumnNode(NonInitialNode):
    __slots__ = 'target_column', 'sep', 'out_columns', 'delete_old'

    def __init__(self, target_column: str = "", sep: str = "", out_columns: Optional[List] = None, delete_old: bool = False):
        super(SplitColumnNode, self).__init__()
        self.target_column = target_column
        self.sep = sep
        self.out_columns = out_columns if out_columns else []
        self.delete_old = delete_old

    def set_attribute(self, *args, **kwargs):
        if 'target_column' in kwargs:
            self.target_column = kwargs['target_column']

        if 'sep' in kwargs:
            self.sep = kwargs['sep']

        if 'out_columns' in kwargs:
            self.out_columns = kwargs['out_columns']

        if 'delete_old' in kwargs:
            self.delete_old = kwargs['delete_old']
