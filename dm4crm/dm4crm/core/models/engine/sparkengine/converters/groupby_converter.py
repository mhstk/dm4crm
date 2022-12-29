from ...baseengine.converter import Converter
from ....node.transform.groupby_node import GroupbyNode


class GroupbyConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: GroupbyNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""
        if node.group_columns:
            code_str += f'{out_port_ident} = {in_port_ident}.groupby({node.group_columns}, as_index=False)'
        if node.agg_functions:
            if not node.group_columns:
                code_str += '\n'
                code_str += f'{out_port_ident} = {in_port_ident}'
            code_str += f'.agg(**{node.agg_functions})\n'
            code_str += f'{out_port_ident}.columns = list(map("".join, {out_port_ident}.columns.values))\n'

        return code_str

