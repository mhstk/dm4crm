from ....node.transform.select_column_node import SelectColumnNode
from ...baseengine.converter import Converter


class SelectColumnConverter(Converter):
    def __init__(self):
        super(SelectColumnConverter, self).__init__()

    def convert(self):
        node: SelectColumnNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""
        if node.selected_columns:
            code_str += f"{out_port_ident} = {in_port_ident}[{str(node.selected_columns)}]"
        if node.alias:
            code_str += f'.rename(columns={node.alias})'
        return code_str
