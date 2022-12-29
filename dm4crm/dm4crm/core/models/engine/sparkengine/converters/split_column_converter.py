from ...baseengine.converter import Converter
from ....node.transform.split_column_node import SplitColumnNode


class SplitColumnConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: SplitColumnNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""
        code_str += f"temp_splitter_{out_port_ident} = pandas.DataFrame()\n"
        if node.sep and node.target_column and node.out_columns:
            code_str += f"temp_splitter_{out_port_ident}[{node.out_columns}] " \
                        f"= {in_port_ident}['{node.target_column}'].str.split('{node.sep}', expand= True)\n"
            code_str += f'{out_port_ident} = pandas' \
                        f'.concat([{in_port_ident}, temp_splitter_{out_port_ident}], axis=1, join="inner")'
        if node.delete_old:
            code_str += "\n"
            code_str += f"{out_port_ident} = {out_port_ident}.loc[:, {out_port_ident}.columns != '{node.target_column}']"
        return code_str

