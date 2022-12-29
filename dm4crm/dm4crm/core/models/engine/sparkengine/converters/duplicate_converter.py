from ...baseengine.converter import Converter
from ....node.transform.duplicate_node import DuplicateNode


class DuplicateConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: DuplicateNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        out_port_ident1 = self.get_node_wrapper().get_out_idents()[1]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""

        code_str += f'{out_port_ident} = {in_port_ident}\n'
        code_str += f'{out_port_ident1} = {in_port_ident}.alias("{out_port_ident1}")'

        return code_str

