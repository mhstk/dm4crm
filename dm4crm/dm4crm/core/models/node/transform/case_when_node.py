from typing import Optional, List

from ..non_initial_node import NonInitialNode


class CaseWhenNode(NonInitialNode):
    __slots__ = 'new_column_name', 'columns', 'rules'

    def __init__(self, new_column_name: str = '', columns: Optional[List] = None, rules: Optional[List] = None):
        super(CaseWhenNode, self).__init__()
        self.new_column_name = new_column_name
        self.columns = columns if columns else []
        self.rules = rules if rules else []

    def set_attribute(self, *args, **kwargs):
        self.new_column_name = kwargs.get("new_column_name", self.new_column_name)
        self.columns = kwargs.get("columns", self.columns)
        self.rules = kwargs.get("rules", self.rules)

