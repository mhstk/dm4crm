from ...baseengine.converter import Converter
from ....node.transform.to_numeric_node import ToNumericNode


class ToNumericConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: ToNumericNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""

        code_str += f'{out_port_ident} = {in_port_ident}.withColumn("{node.column}",col("{node.column}").cast("float"))'


        return code_str

