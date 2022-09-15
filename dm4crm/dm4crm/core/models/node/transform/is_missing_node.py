from ..non_initial_node import NonInitialNode


class IsMissingNode(NonInitialNode):

    def __init__(self):
        super(IsMissingNode, self).__init__()

    def set_attribute(self, *args, **kwargs):
        pass
