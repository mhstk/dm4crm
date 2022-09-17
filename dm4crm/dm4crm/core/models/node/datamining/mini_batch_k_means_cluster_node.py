from ..non_initial_node import NonInitialNode


class MiniBatchKMeansClusterNode(NonInitialNode):
    __slots__ = \
        'n_clusters' \
        'init' \
        'max_iter' \
        'batch_size'

    def __init__(self,
                 n_clusters: int = 8,
                 init: str = 'k-means++',
                 max_iter: int = 100,
                 batch_size: int = 1024):
        super(MiniBatchKMeansClusterNode, self).__init__()

        self.n_clusters = n_clusters
        self.init = init
        self.max_iter = max_iter
        self.batch_size = batch_size

        self.set_in_port(None, 1)

    def set_attribute(self, *args, **kwargs):
        self.n_clusters = kwargs.get('n_clusters', self.n_clusters)
        self.init = kwargs.get('init', self.init)
        self.max_iter = kwargs.get('max_iter', self.max_iter)
        self.batch_size = kwargs.get('batch_size', self.batch_size)
