from ....node.transform.change_column_type_node import ChangeColumnTypeNode
from ...baseengine.converter import Converter


class ChangeColumnTypeConverter(Converter):
    def __init__(self):
        super(ChangeColumnTypeConverter, self).__init__()

    def convert(self):
        node: ChangeColumnTypeNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""
        if node.new_type:
            code_str += f'{out_port_ident} = {in_port_ident}.astype({node.new_type})'
        return code_str
