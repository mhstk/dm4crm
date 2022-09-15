from ...baseengine.converter import Converter
from ....node.transform.concat_node import ConcatNode


class ConcatConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: ConcatNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        in_port_ident1 = self.get_node_wrapper().get_in_idents()[1]
        code_str = ""

        code_str += f'{out_port_ident} = pandas.concat([{in_port_ident}, {in_port_ident1}], axis=1, join="inner")'

        return code_str

