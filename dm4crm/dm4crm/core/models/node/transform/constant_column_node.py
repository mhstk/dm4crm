from ..non_initial_node import NonInitialNode


class ConstantColumnNode(NonInitialNode):
    __slots__ = 'column_name', 'value', 'col_type'

    def __init__(self, column_name: str = '', value: str = '', col_type: str = 'str'):
        super(ConstantColumnNode, self).__init__()
        self.column_name = column_name
        self.value = value
        self.col_type: str = col_type

    def set_attribute(self, *args, **kwargs):
        if 'column_name' in kwargs:
            self.column_name = kwargs['column_name']

        if 'value' in kwargs:
            print("here", kwargs['value'])
            self.value = kwargs['value']

        if 'col_type' in kwargs:
            self.col_type = kwargs['col_type']

