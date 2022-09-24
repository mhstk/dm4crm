from ..non_initial_node import NonInitialNode


class DecisionTreeClassifierNode(NonInitialNode):
    __slots__ = ('max_depth', )

    def __init__(self, max_depth: int = 5):
        super(DecisionTreeClassifierNode, self).__init__()
        self.max_depth = max_depth
        self.set_in_port(None, 1)

    def set_attribute(self, *args, **kwargs):
        self.max_depth = kwargs.get("max_depth", self.max_depth)

