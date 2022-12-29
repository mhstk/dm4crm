from ...baseengine.converter import Converter
from ....node.transform.drop_na_node import DropNaNode


class DropNaConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: DropNaNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""

        code_str += f'{out_port_ident} = {in_port_ident}.na.drop()'

        return code_str

