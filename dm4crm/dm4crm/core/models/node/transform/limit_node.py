from ..non_initial_node import NonInitialNode


class LimitNode(NonInitialNode):
    __slots__ = 'offset_num', 'limit_num'

    def __init__(self, offset_num: int = 0, limit_num: int = 20):
        super(LimitNode, self).__init__()
        self.offset_num: int = offset_num
        self.limit_num: int = limit_num

    def set_limit_num(self, limit_num: int):
        self.limit_num = limit_num

    def set_offset_num(self, offset_num: int):
        self.offset_num = offset_num

    def set_attribute(self, *args, **kwargs):
        if 'offset_num' in kwargs:
            self.offset_num = kwargs['offset_num']

        if 'limit_num' in kwargs:
            self.limit_num = kwargs['limit_num']





