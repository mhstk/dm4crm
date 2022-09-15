from typing import List, Optional, Dict

from ..non_initial_node import NonInitialNode


class SelectColumnNode(NonInitialNode):
    __slots__ = 'selected_columns', 'alias'

    def __init__(self, selected_columns: Optional[List[str]] = None, alias: Optional[Dict] = None):
        super(SelectColumnNode, self).__init__()
        self.selected_columns = selected_columns if selected_columns else []
        self.alias = alias if alias else {}

    def set_attribute(self, *args, **kwargs):
        if 'selected_columns' in kwargs:
            self.selected_columns = kwargs['selected_columns']

        if 'alias' in kwargs:
            self.alias = kwargs['alias']
