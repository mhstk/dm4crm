from ..non_initial_node import NonInitialNode


class UnionNode(NonInitialNode):
    __slots__ = NonInitialNode.__slots__

    def __init__(self):
        super(UnionNode, self).__init__()
        self.set_in_port(None, 1)

    def set_attribute(self, *args, **kwargs):
        pass
