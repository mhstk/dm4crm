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

        code_str += f'{out_port_ident} = RandomForestClassifier(    '
        if node.n_estimators:
            code_str += f'n_estimators={node.n_estimators}  ,'
        if node.criterion:
            code_str += f'criterion="{node.criterion}"  ,'
        if node.max_depth:
            code_str += f'max_depth={node.max_depth}  ,'
        if node.min_samples_split:
            code_str += f'min_samples_split={node.min_samples_split}  ,'
        if node.min_samples_leaf:
            code_str += f'min_samples_leaf={node.min_samples_leaf}  ,'
        if node.min_weight_fraction_leaf:
            code_str += f'min_weight_fraction_leaf={node.min_weight_fraction_leaf}  ,'
        if node.max_features:
            if isinstance(node.max_features, str):
                code_str += f'max_features="{node.max_features}"  ,'
            else:
                code_str += f'max_features={node.max_features}  ,'
        if node.max_leaf_nodes:
            code_str += f'max_leaf_nodes={node.max_leaf_nodes}  ,'
        if node.min_impurity_decrease:
            code_str += f'min_impurity_decrease={node.min_impurity_decrease}  ,'
        if node.bootstrap:
            code_str += f'bootstrap={node.bootstrap}  ,'
        if node.oob_score:
            code_str += f'oob_score={node.oob_score}  ,'
        if node.n_jobs:
            code_str += f'n_jobs={node.n_jobs}  ,'
        code_str = code_str[:-1]
        code_str += f').fit({in_port_ident}, {in_port_ident1}.values.ravel())'
        return code_str

