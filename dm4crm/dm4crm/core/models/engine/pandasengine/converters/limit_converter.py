from ....node.transform.limit_node import LimitNode
from ...baseengine.converter import Converter


class LimitConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: LimitNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""
        if node.limit_num:
            code_str += f"{out_port_ident} = {in_port_ident}" \
                        f".iloc[{node.offset_num}:{node.limit_num}].reset_index(drop=True)"
        return code_str

