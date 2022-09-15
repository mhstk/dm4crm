from ...baseengine.converter import Converter
from ....node.transform.join_node import JoinNode


class JoinConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: JoinNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        in_port_ident_1 = self.get_node_wrapper().get_in_idents()[1]
        code_str = ""
        code_str += f'{out_port_ident} = {in_port_ident}.merge({in_port_ident_1}'
        if node.join_type:
            code_str += f', how="{node.join_type}"'
        if node.left_on:
            code_str += f', left_on={node.left_on}'
        if node.right_on:
            code_str += f', right_on={node.right_on}'
        code_str += ')'

        return code_str

