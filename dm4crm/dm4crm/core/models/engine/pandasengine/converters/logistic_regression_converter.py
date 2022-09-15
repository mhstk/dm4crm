from ...baseengine.converter import Converter
from ....node.datamining.logistic_regression_node import LogisticRegressionNode


class LogisticRegressionConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: LogisticRegressionNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        in_port_ident_1 = self.get_node_wrapper().get_in_idents()[1]
        code_str = ""

        code_str += 'from sklearn.linear_model import LogisticRegression\n'
        code_str += f'{out_port_ident} = LogisticRegression(random_state=0, solver="lbfgs", multi_class="ovr")' \
                    f'.fit({in_port_ident}, {in_port_ident_1}.values.ravel())'


        return code_str

