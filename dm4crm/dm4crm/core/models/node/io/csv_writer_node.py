from ..non_initial_node import NonInitialNode


class CSVWriterNode(NonInitialNode):
    __slots__ = 'file_name', 'sep', 'encoding'

    def __init__(self, file_name: str = '', sep: str = ',', encoding: str = 'utf-8'):
        super(CSVWriterNode, self).__init__()
        self.file_name = file_name
        self.sep = sep
        self.encoding = encoding

    def set_attribute(self, *args, **kwargs):
        self.file_name = kwargs.get('file_name', self.file_name)
        self.sep = kwargs.get('sep', self.sep)
        self.encoding = kwargs.get('encoding', self.encoding)
