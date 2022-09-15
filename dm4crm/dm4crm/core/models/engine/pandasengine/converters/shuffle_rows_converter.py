from ...baseengine.converter import Converter
from ....node.transform.shuffle_rows_node import ShuffleRowsNode


class ShuffleRowsConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: ShuffleRowsNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""
        code_str += f"{out_port_ident} = {in_port_ident}.sample(frac=1).reset_index(drop=True)"
        return code_str

