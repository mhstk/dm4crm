from ..non_initial_node import NonInitialNode


class PredictNode(NonInitialNode):
    __slots__ = 'target_name',

    def __init__(self, target_name: str = ''):
        super(PredictNode, self).__init__()
        self.target_name = target_name
        self.set_in_port(None, 1)

    def set_attribute(self, *args, **kwargs):
        self.target_name = kwargs.get("target_name", self.target_name)


