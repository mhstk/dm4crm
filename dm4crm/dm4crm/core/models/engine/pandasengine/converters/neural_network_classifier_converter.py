from ...baseengine.converter import Converter
from ....node.datamining.neural_network_classifier_node import NeuralNetworkClassifierNode


class NeuralNetworkClassifierConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: NeuralNetworkClassifierNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        in_port_ident1 = self.get_node_wrapper().get_in_idents()[1]
        code_str = ""

        code_str += 'from sklearn.neural_network import MLPClassifier\n'
        code_str += f'{out_port_ident} = MLPClassifier(  '
        if node.hidden_layer_sizes:
            code_str += f'hidden_layer_sizes={node.hidden_layer_sizes} ,'
        if node.activation:
            code_str += f'activation="{node.activation}" ,'
        if node.solver:
            code_str += f'solver="{node.solver}" ,'
        if node.alpha:
            code_str += f'alpha={node.alpha} ,'
        if node.batch_size:
            if isinstance(node.batch_size, str):
                code_str += f'batch_size="{node.batch_size}" ,'
            else:
                code_str += f'batch_size={node.batch_size} ,'
        if node.learning_rate:
            code_str += f'learning_rate="{node.learning_rate}" ,'
        if node.learning_rate_init:
            code_str += f'learning_rate_init={node.learning_rate_init} ,'
        if node.power_t:
            code_str += f'power_t={node.power_t} ,'
        if node.max_iter:
            code_str += f'max_iter={node.max_iter} ,'
        if node.shuffle:
            code_str += f'shuffle={node.shuffle} ,'

        code_str = code_str[:-1]
        code_str += f').fit({in_port_ident}, {in_port_ident1}.values.ravel())'

        return code_str

