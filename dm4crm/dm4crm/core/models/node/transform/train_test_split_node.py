from typing import Union

from ..non_initial_node import NonInitialNode


class TrainTestSplitNode(NonInitialNode):
    __slots__ = 'train_size',

    def __init__(self, train_size: Union[int, float] = 0.75):
        super(TrainTestSplitNode, self).__init__()
        self.set_out_port(None, 0)
        self.set_out_port(None, 1)
        self.train_size = train_size

    def set_attribute(self, *args, **kwargs):
        self.train_size = kwargs.get("train_size", self.train_size)

