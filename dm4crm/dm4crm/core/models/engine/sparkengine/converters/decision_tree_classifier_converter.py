from ...baseengine.converter import Converter
from ....node.datamining.decision_tree_classifier_node import DecisionTreeClassifierNode


class DecisionTreeClassifierConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: DecisionTreeClassifierNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        in_port_ident_1 = self.get_node_wrapper().get_in_idents()[1]
        code_str = ""
        code_str += f'''from pyspark.ml.feature import VectorAssembler
{in_port_ident}_temp = VectorAssembler(inputCols = {in_port_ident}.columns, outputCol='features').transform({in_port_ident})
temp_column_name = {in_port_ident_1}.columns[0]
{in_port_ident_1}_temp = {in_port_ident_1}.withColumn("label", col(f"{{temp_column_name}}"))\n'''
        code_str += 'from pyspark.ml.classification import DecisionTreeClassifier\n'
        code_str += f'{out_port_ident} = DecisionTreeClassifier().fit(spark_concat({in_port_ident}_temp, {in_port_ident_1}_temp))'

        return code_str

