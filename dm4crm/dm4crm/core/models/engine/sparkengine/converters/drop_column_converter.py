from ...baseengine.converter import Converter
from ....node.transform.drop_column_node import DropColumnNode


class DropColumnConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: DropColumnNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""
        if len(node.target_column) > 0:
            code_str += f'{out_port_ident} = {in_port_ident}.drop{*node.target_column,}'

        return code_str

