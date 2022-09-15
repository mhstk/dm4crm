from ...baseengine.converter import Converter
from ....node.datamining.score_node import ScoreNode


class ScoreConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: ScoreNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        in_port_ident1 = self.get_node_wrapper().get_in_idents()[1]
        code_str = ""
        code_str += "from sklearn.metrics import accuracy_score, confusion_matrix\n"
        code_str += f'{out_port_ident} = {{ "accuracy_score": ' \
                    f'accuracy_score({in_port_ident}.values, {in_port_ident1}.values),' \
                    f'"confusion_matrix": ' \
                    f'confusion_matrix({in_port_ident1}.values, {in_port_ident}.values).tolist()}}'

        return code_str

