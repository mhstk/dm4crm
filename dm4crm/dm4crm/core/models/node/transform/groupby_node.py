from typing import List, Dict, Optional

from ..non_initial_node import NonInitialNode


class GroupbyNode(NonInitialNode):
    __slots__ = 'group_columns', 'agg_functions'

    def __init__(self, group_columns: Optional[List[str]] = None, agg_functions: Optional[Dict] = None):
        super(GroupbyNode, self).__init__()
        self.group_columns: List[str] = group_columns if group_columns else []
        self.agg_functions: Dict = agg_functions if agg_functions else {}

    def set_attribute(self, *args, **kwargs):
        self.group_columns = kwargs.get('group_columns', self.group_columns)
        self.agg_functions = kwargs.get('agg_functions', self.agg_functions)

