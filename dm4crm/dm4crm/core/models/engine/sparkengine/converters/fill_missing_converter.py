from ...baseengine.converter import Converter
from ....node.transform.fill_missing_node import FillMissingNode


class FillMissingConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: FillMissingNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""

        if node.value:
            if isinstance(node.value, str):
                code_str += f'{out_port_ident} = {in_port_ident}.fillna("{node.value}"'
            else:
                code_str += f'{out_port_ident} = {in_port_ident}.fillna({node.value}'
            if node.method:
                code_str += f', method="{node.method}"'
            code_str += ')'
        elif node.method:
            code_str += f'{out_port_ident} = {in_port_ident}.fillna(method="{node.method}")'

        return code_str

