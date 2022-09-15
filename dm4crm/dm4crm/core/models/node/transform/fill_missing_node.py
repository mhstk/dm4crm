from typing import Optional, Dict, Union
from ..non_initial_node import NonInitialNode


class FillMissingNode(NonInitialNode):
    __slots__ = 'value', 'method'

    def __init__(self, value: Optional[Union[Dict, str, int]] = 0, method: str = ''):
        super(FillMissingNode, self).__init__()
        self.value = value
        self.method = method

    def set_attribute(self, *args, **kwargs):
        self.value = kwargs.get("value", self.value)
        self.method = kwargs.get('method', self.method)
