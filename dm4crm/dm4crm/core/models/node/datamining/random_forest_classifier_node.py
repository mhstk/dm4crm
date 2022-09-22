from typing import Optional, Union

from ..non_initial_node import NonInitialNode


class RandomForestClassifierNode(NonInitialNode):
    __slots__ = \
        'n_estimators', \
        'criterion', \
        'max_depth', \
        'min_samples_split', \
        'min_samples_leaf', \
        'min_weight_fraction_leaf', \
        'max_features', \
        'max_leaf_nodes', \
        'min_impurity_decrease', \
        'bootstrap', \
        'oob_score', \
        'n_jobs'

    def __init__(self,
                 n_estimators: int = 100,
                 criterion: str = 'gini',
                 max_depth: Optional[int] = None,
                 min_samples_split: Union[int, float] = 2,
                 min_samples_leaf: Union[int, float] = 1,
                 min_weight_fraction_leaf: float = 0.0,
                 max_features: Optional[str] = 'sqrt',
                 max_leaf_nodes: Optional[int] = None,
                 min_impurity_decrease: float = 0.0,
                 bootstrap: bool = True,
                 oob_score: bool = False,
                 n_jobs: Optional[int] = None
                 ):
        super(RandomForestClassifierNode, self).__init__()
        self.n_estimators = n_estimators
        self.criterion = criterion
        self.max_depth = max_depth
        self.min_samples_split = min_samples_split
        self.min_samples_leaf = min_samples_leaf
        self.min_weight_fraction_leaf = min_weight_fraction_leaf
        self.max_features = max_features
        self.max_leaf_nodes = max_leaf_nodes
        self.min_impurity_decrease = min_impurity_decrease
        self.bootstrap = bootstrap
        self.oob_score = oob_score
        self.n_jobs = n_jobs

        self.set_in_port(None, 1)

    def set_attribute(self, *args, **kwargs):
        self.n_estimators = kwargs.get('n_estimators', self.n_estimators)
        self.criterion = kwargs.get('criterion', self.criterion)
        self.max_depth = kwargs.get('max_depth', self.max_depth)
        self.min_samples_split = kwargs.get('min_samples_split', self.min_samples_split)
        self.min_samples_leaf = kwargs.get('min_samples_leaf', self.min_samples_leaf)
        self.min_weight_fraction_leaf = kwargs.get('min_weight_fraction_leaf', self.min_weight_fraction_leaf)
        self.max_features = kwargs.get('max_features', self.max_features)
        self.max_leaf_nodes = kwargs.get('max_leaf_nodes', self.max_leaf_nodes)
        self.min_impurity_decrease = kwargs.get('min_impurity_decrease', self.min_impurity_decrease)
        self.bootstrap = kwargs.get('bootstrap', self.bootstrap)
        self.oob_score = kwargs.get('oob_score', self.oob_score)
        self.n_jobs = kwargs.get('n_jobs', self.n_jobs)
