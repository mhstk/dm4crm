from ...baseengine.converter import Converter
from ....node.datamining.decision_tree_classifier_node import DecisionTreeClassifierNode


class DecisionTreeClassifierConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: DecisionTreeClassifierNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        in_port_ident_1 = self.get_node_wrapper().get_in_idents()[1]
        code_str = ""

        code_str += 'from sklearn.tree import DecisionTreeClassifier\n'
        code_str += f'{out_port_ident} = DecisionTreeClassifier(max_depth={node.max_depth})' \
                    f'.fit({in_port_ident}, {in_port_ident_1}.values.ravel())'

        return code_str

