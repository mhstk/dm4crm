from ...baseengine.converter import Converter
from ....node.datamining.score_node import ScoreNode


class ScoreConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: ScoreNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""


        return code_str

