from ..non_initial_node import NonInitialNode


class ShuffleRowsNode(NonInitialNode):
    __slots__ = NonInitialNode.__slots__

    def __init__(self):
        super(ShuffleRowsNode, self).__init__()

    def set_attribute(self, *args, **kwargs):
        pass
