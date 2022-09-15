from ...baseengine.converter import Converter
from ....node.transform.train_test_split_node import TrainTestSplitNode


class TrainTestSplitConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: TrainTestSplitNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        out_port_ident1 = self.get_node_wrapper().get_out_idents()[1]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""
        code_str += 'from sklearn.model_selection import train_test_split\n'
        code_str += f'{out_port_ident}, {out_port_ident1} = train_test_split({in_port_ident}, ' \
                    f'train_size={node.train_size})'
        return code_str

