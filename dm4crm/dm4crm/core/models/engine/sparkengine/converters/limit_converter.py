from ....node.transform.limit_node import LimitNode
from ...baseengine.converter import Converter


class LimitConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: LimitNode = self.get_node_wrapper().get_node()
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        code_str = ""
        if node.limit_num:
            code_str += f"{out_port_ident} = {in_port_ident}" \
                        f".limit({node.limit_num})"
        return code_str

