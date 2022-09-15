from ....node import SortNode
from ...baseengine.converter import Converter


class SortConverter(Converter):
    def __init__(self):
        super(SortConverter, self).__init__()

    def convert(self):
        node: SortNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""
        if node.selected_columns:
            code_str += f'{out_port_ident} = {in_port_ident}' \
                        f'.sort_values(by={node.selected_columns}, ascending={node.ascending}).reset_index(drop=True)'
        return code_str
