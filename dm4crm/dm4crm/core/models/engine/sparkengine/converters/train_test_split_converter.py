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

        code_str += f'total = {1 if isinstance(node.train_size, float) else f"{in_port_ident}.count()"}\n'
        code_str += f'{out_port_ident}, {out_port_ident1} = {in_port_ident}' \
                    f'.randomSplit([{float(node.train_size)}, float(total - {node.train_size})])'

        return code_str

