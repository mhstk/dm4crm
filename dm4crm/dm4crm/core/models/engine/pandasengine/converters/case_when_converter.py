from ...baseengine.converter import Converter
from ....node.transform.case_when_node import CaseWhenNode


class CaseWhenConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: CaseWhenNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""
        columns_name = ', '.join(node.columns)

        code_str += f'{out_port_ident} = {in_port_ident}\n'
        code_str += f'def func_{in_port_ident}{out_port_ident}{node.new_column_name}({columns_name}):\n' \

        for cond, res in node.rules:
            code_str += f'    if {cond}:\n' \
                        f'        return {res}\n'
        code_str += "\n\n"
        column_name = ", ".join(["x."+str(y) for y in node.columns])
        code_str += f'{out_port_ident}["{node.new_column_name}"] = {out_port_ident}' \
                    f'.apply(lambda x : func_{in_port_ident}{out_port_ident}{node.new_column_name}' \
                    f'({column_name}), axis = 1)'

        return code_str

