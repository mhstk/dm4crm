from ...baseengine.converter import Converter
from ....node.transform.constant_column_node import ConstantColumnNode


class ConstantColumnConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: ConstantColumnNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""
        if node.column_name and node.value and node.col_type:
            code_str += f'{in_port_ident}["{node.column_name}"] = "{node.value}"\n'
            code_str += f'{out_port_ident} = {in_port_ident}.astype({{"{node.column_name}":"{node.col_type}"}})'
        if node.column_name and node.value is None:
            code_str += f'{in_port_ident}["{node.column_name}"] = np.nan\n'
            code_str += f'{out_port_ident} = {in_port_ident}'

        return code_str
