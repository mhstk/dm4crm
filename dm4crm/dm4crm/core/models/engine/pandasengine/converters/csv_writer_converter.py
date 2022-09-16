from ...baseengine.converter import Converter
from ....node.io.csv_writer_node import CSVWriterNode


class CSVWriterConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: CSVWriterNode = self.get_node_wrapper().get_node()
        # out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""

        code_str += f'{in_port_ident}' \
                    f'.to_csv(r"{node.file_name}", sep="{node.sep}", encoding="{node.encoding}", index=False)'
        return code_str

