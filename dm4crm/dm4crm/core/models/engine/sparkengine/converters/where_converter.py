from ...baseengine.converter import Converter
from ....node.transform.where_node import WhereNode


class WhereConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: WhereNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""

        if node.query:
            code_str += f'{out_port_ident} = {in_port_ident}.query("{node.query}")'

        return code_str

