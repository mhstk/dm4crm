from typing import Dict, Optional

from ..non_initial_node import NonInitialNode


class ChangeColumnTypeNode(NonInitialNode):
    __slots__ = 'new_type'

    def __init__(self, new_type: Optional[Dict] = None):
        super(ChangeColumnTypeNode, self).__init__()
        self.new_type = new_type if new_type else {}

    def set_attribute(self, *args, **kwargs):
        if 'new_type' in kwargs:
            self.new_type = kwargs['new_type']

