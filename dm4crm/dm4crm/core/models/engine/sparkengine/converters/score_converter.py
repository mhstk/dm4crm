from ...baseengine.converter import Converter
from ....node.datamining.score_node import ScoreNode


class ScoreConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: ScoreNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        in_port_ident1 = self.get_node_wrapper().get_in_idents()[1]
        code_str = ""
        code_str += f'''temp_column_name = {in_port_ident}.columns[0]
{in_port_ident}_temp = {in_port_ident}.withColumn("prediction", col(f"{{temp_column_name}}").cast("float"))
temp_column_name = {in_port_ident1}.columns[0]
{in_port_ident1}_temp = {in_port_ident1}.withColumn("label", col(f"{{temp_column_name}}").cast("float"))
{out_port_ident}_temp = spark_concat({in_port_ident}_temp, {in_port_ident1}_temp).select("prediction", "label")

from pyspark.mllib.evaluation import MulticlassMetrics
metrics = MulticlassMetrics({out_port_ident}_temp.rdd)
{out_port_ident} = {{ "accuracy_score":  \n
                    metrics.accuracy, \n
                    "confusion_matrix":  \n
                    metrics.confusionMatrix().toArray().tolist()}}'''

        return code_str

