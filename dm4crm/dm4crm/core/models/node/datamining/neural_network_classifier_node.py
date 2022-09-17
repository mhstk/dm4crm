from typing import Tuple, Union

from ..non_initial_node import NonInitialNode


class NeuralNetworkClassifierNode(NonInitialNode):
    __slots__ = \
        'hidden_layer_sizes', \
        'activation', \
        'solver', \
        'alpha', \
        'batch_size', \
        'learning_rate', \
        'learning_rate_init', \
        'power_t', \
        'max_iter', \
        'shuffle'

    def __init__(self,
                 hidden_layer_sizes: Tuple = (100,),
                 activation: str = 'relu',
                 solver: str = 'adam',
                 alpha: float = 0.0001,
                 batch_size: Union[str, int] = 'auto',
                 learning_rate: str = 'constant',
                 learning_rate_init: float = 0.001,
                 power_t: float = 0.5,
                 max_iter: int = 200,
                 shuffle: bool = True):
        super(NeuralNetworkClassifierNode, self).__init__()
        self.hidden_layer_sizes = hidden_layer_sizes
        self.activation = activation
        self.solver = solver
        self.alpha = alpha
        self.batch_size = batch_size
        self.learning_rate = learning_rate
        self.learning_rate_init = learning_rate_init
        self.power_t = power_t
        self.max_iter = max_iter
        self.shuffle = shuffle

        self.set_in_port(None, 1)

    def set_attribute(self, *args, **kwargs):
        self.hidden_layer_sizes = kwargs.get('hidden_layer_sizes', self.hidden_layer_sizes)
        self.activation = kwargs.get('activation', self.activation)
        self.solver = kwargs.get('solver', self.solver)
        self.alpha = kwargs.get('alpha', self.alpha)
        self.batch_size = kwargs.get('batch_size', self.batch_size)
        self.learning_rate = kwargs.get('learning_rate', self.learning_rate)
        self.learning_rate_init = kwargs.get('learning_rate_init', self.learning_rate_init)
        self.power_t = kwargs.get('power_t', self.power_t)
        self.max_iter = kwargs.get('max_iter', self.max_iter)
        self.shuffle = kwargs.get('shuffle', self.shuffle)
