from ...baseengine.converter import Converter
from ....node.datamining.random_forest_classifier_node import RandomForestClassifierNode


class RandomForestClassifierConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: RandomForestClassifierNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        in_port_ident1 = self.get_node_wrapper().get_in_idents()[1]
        code_str = ""
        code_str += "from sklearn.ensemble import RandomForestClassifier\n"

        code_str += f'{out_port_ident} = RandomForestClassifier' \
                    f'(n_estimators={node.n_estimators}, ' \
                    f'max_features={node.max_features}, ' \
                    f'max_depth={node.max_depth})' \
                    f'.fit({in_port_ident}, {in_port_ident1}.values.ravel())'
        return code_str

