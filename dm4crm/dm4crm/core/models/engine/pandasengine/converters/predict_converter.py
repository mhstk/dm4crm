from ...baseengine.converter import Converter
from ....node.datamining.predict_node import PredictNode


class PredictConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: PredictNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        in_port_ident_1 = self.get_node_wrapper().get_in_idents()[1]
        code_str = ''

        code_str += f'{out_port_ident}_temp = {in_port_ident}.predict({in_port_ident_1})\n'
        code_str += f'{out_port_ident} = pandas.DataFrame({out_port_ident}_temp, columns=["{node.target_name}_PREDICTION"])'

        return code_str

