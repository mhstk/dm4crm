from ..initial_node import InitialNode


class CSVReaderNode(InitialNode):
    __slots__ = 'file_path', 'read_header'

    def __init__(self, file_path: str = "", read_header: bool = True) -> None:
        super().__init__()
        self.file_path = file_path
        self.read_header = read_header

    def set_attribute(self, *args, **kwargs):
        if 'file_path' in kwargs:
            self.file_path = kwargs['file_path']

        if 'read_header' in kwargs:
            self.read_header = kwargs['read_header']
