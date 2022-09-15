from typing import List, Optional

from ..non_initial_node import NonInitialNode


class JoinNode(NonInitialNode):
    __slots__ = 'join_type', 'left_on', 'right_on'

    def __init__(self, join_type: str = '', left_on: Optional[List] = None, right_on: Optional[List] = None):
        super(JoinNode, self).__init__()
        self.join_type = join_type
        self.left_on = left_on if left_on else []
        self.right_on = right_on if right_on else []

    def set_attribute(self, *args, **kwargs):
        self.join_type = kwargs.get('join_type', self.join_type)
        self.left_on = kwargs.get('left_on', self.left_on)
        self.right_on = kwargs.get('right_on', self.right_on)

