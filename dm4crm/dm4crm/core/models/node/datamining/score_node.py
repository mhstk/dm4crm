from ..non_initial_node import NonInitialNode


class ScoreNode(NonInitialNode):

    def __init__(self):
        super(ScoreNode, self).__init__()
        self.set_in_port(None, 1)

    def set_attribute(self, *args, **kwargs):
        pass
