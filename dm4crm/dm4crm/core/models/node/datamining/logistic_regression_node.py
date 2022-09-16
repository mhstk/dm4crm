from ..non_initial_node import NonInitialNode


class LogisticRegressionNode(NonInitialNode):
    __slots__ = NonInitialNode.__slots__

    def __init__(self):
        super(LogisticRegressionNode, self).__init__()

    def set_attribute(self, *args, **kwargs):
        pass


