from ..non_initial_node import NonInitialNode


class WhereNode(NonInitialNode):
    __slots__ = 'query',

    def __init__(self, query: str = ''):
        super(WhereNode, self).__init__()
        self.query = query

    def set_attribute(self, *args, **kwargs):
        self.query = kwargs.get("query", self.query)


