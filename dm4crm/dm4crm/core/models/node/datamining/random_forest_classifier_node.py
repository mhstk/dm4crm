from ..non_initial_node import NonInitialNode


class RandomForestClassifierNode(NonInitialNode):
    __slots__ = 'max_depth', 'n_estimators', 'max_features'

    def __init__(self, max_depth: int = 5, n_estimators: int = 10, max_features: int = 1):
        super(RandomForestClassifierNode, self).__init__()
        self.max_depth = max_depth
        self.n_estimators = n_estimators
        self.max_features = max_features
        self.set_in_port(None, 1)

    def set_attribute(self, *args, **kwargs):
        self.max_depth = kwargs.get('max_depth', self.max_depth)
        self.n_estimators = kwargs.get('n_estimators', self.n_estimators)
        self.max_features = kwargs.get('max_features', self.max_features)

