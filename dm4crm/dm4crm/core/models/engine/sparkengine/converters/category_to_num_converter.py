from ...baseengine.converter import Converter
from ....node.transform.category_to_num_node import CategoryToNumNode


class CategoryToNumConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: CategoryToNumNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""

        code_str += f'''import itertools

def spark_get_dummies{in_port_ident}{out_port_ident}(df, columns):
    non_nominal_columns = [x for x in df.columns if x not in set(columns)]
    categories = []
    for i, values in enumerate(columns):
        categories.append(df.select(values).distinct().rdd.flatMap(lambda x: x).collect())   
    expressions = []
    for i, values in enumerate(columns):
        expressions.append([when(col(values) == i, 1).otherwise(0).alias(str(values) + "_" + str(i)) for i in categories[i]])
    expressions_flat = list(itertools.chain.from_iterable(expressions))
    df_final = df.select(*expressions_flat+non_nominal_columns)
    return df_final\n'''

        code_str += f'columnList = [item[0] for item in {in_port_ident}.dtypes if item[1].startswith("string")]\n'
        code_str += f'{out_port_ident} = spark_get_dummies{in_port_ident}{out_port_ident}({in_port_ident}, columnList)'

        return code_str

