from ...baseengine.converter import Converter
from ....node.transform.separate_target_column_node import SeparateTargetColumnNode


class SeparateTargetColumnConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: SeparateTargetColumnNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        out_port_ident1 = self.get_node_wrapper().get_out_idents()[1]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""

        code_str += f'{out_port_ident} = {in_port_ident}.drop({node.target_column}, axis=1)\n'
        code_str += f'{out_port_ident1} = {in_port_ident}{node.target_column}\n'

        return code_str

