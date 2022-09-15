from typing import List, Optional

from ..non_initial_node import NonInitialNode


class SortNode(NonInitialNode):
    __slots__ = 'selected_columns', 'ascending'

    def __init__(self, selected_columns: Optional[List[str]] = None, ascending: bool = True):
        super(SortNode, self).__init__()
        self.selected_columns = selected_columns if selected_columns else []
        self.ascending = ascending

    def set_attribute(self, *args, **kwargs):
        if 'selected_columns' in kwargs:
            self.selected_columns = kwargs['selected_columns']

        if 'ascending' in kwargs:
            self.ascending = kwargs['ascending']
