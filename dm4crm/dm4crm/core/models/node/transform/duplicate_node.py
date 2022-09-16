from ..non_initial_node import NonInitialNode


class DuplicateNode(NonInitialNode):
    __slots__ = NonInitialNode.__slots__

    def __init__(self):
        super(DuplicateNode, self).__init__()
        self.set_out_port(None, 1)

    def set_attribute(self, *args, **kwargs):
        pass
