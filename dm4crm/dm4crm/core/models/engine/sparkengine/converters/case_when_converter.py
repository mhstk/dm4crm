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

        rule_str = ''
        for i, (cond, res), in enumerate(node.rules):
            if i == 0:
                rule_str += f'when(expr("{cond}"), {res})'
            else:
                rule_str += f'.when(expr("{cond}"), {res})'

        code_str += f'{out_port_ident} = {in_port_ident}\\\n' +\
                    f'\t.withColumn("{node.new_column_name}", {rule_str})'

        return code_str

