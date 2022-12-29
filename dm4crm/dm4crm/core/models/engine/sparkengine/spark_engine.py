import json
from typing import cast, Any, Dict, Optional

from .converters import *
from ...execution.executor import Executor
from ...node import *
from ..baseengine.base_engine import BaseEngine
from ..baseengine.converter import Converter
from ...execution.spark_local_execution_handler import SparkLocalExecutionHandler
from ...schema.schema import Schema


class SparkEngine(BaseEngine):

    def __init__(self) -> None:
        super().__init__()
        self.execution_handler: SparkLocalExecutionHandler = cast(SparkLocalExecutionHandler, self.execution_handler)
        self.node_converter_map: Dict = {LimitNode: LimitConverter,
                                         CSVReaderNode: CSVReaderConverter,
                                         MysqlReaderNode: MysqlReaderConverter,
                                         CSVWriterNode: CSVWriterConverter,
                                         SelectColumnNode: SelectColumnConverter,
                                         ChangeColumnTypeNode: ChangeColumnTypeConverter,
                                         SortNode: SortConverter,
                                         ShuffleRowsNode: ShuffleRowsConverter,
                                         SplitColumnNode: SplitColumnConverter,
                                         ConstantColumnNode: ConstantColumnConverter,
                                         GroupbyNode: GroupbyConverter,
                                         IsMissingNode: IsMissingConverter,
                                         FillMissingNode: FillMissingConverter,
                                         JoinNode: JoinConverter,
                                         UnionNode: UnionConverter,
                                         CaseWhenNode: CaseWhenConverter,
                                         WhereNode: WhereConverter,
                                         DuplicateNode: DuplicateConverter,
                                         ConcatNode: ConcatConverter,
                                         SeparateTargetColumnNode: SeparateTargetColumnConverter,
                                         DropColumnNode: DropColumnConverter,
                                         TrainTestSplitNode: TrainTestSplitConverter,
                                         LogisticRegressionNode: LogisticRegressionConverter,
                                         DecisionTreeClassifierNode: DecisionTreeClassifierConverter,
                                         RandomForestClassifierNode: RandomForestClassifierConverter,
                                         NeuralNetworkClassifierNode: NeuralNetworkClassifierConverter,
                                         ScoreNode: ScoreConverter,
                                         PredictNode: PredictConverter,
                                         AccountsCrmNode: MysqlReaderConverter,
                                         CampaignsCrmNode: MysqlReaderConverter,
                                         CategoryToNumNode: CategoryToNumConverter,
                                         ToNumericNode: ToNumericConverter,
                                         DropNaNode: DropNaConverter

                                         }

    def set_spark_home(self, spark_home: str):
        self.execution_handler.spark_home = spark_home

    def set_temp_dir(self, temp_dir: str):
        self.execution_handler.temp_dir = temp_dir

    def dispatcher(self, node: GeneralNode) -> Converter:
        if type(node) in self.node_converter_map:
            return self.node_converter_map[type(node)]()
        else:
            raise Exception(f"Convertor for this node wasn't found: {node}")

    def convert_code(self, nodes: Optional[Dict] = None) -> str:
        code_str: str = ""
        for node_wrapper in self.dataflow.wrap_nodes:
            converter: Converter = self.dispatcher(node_wrapper.get_node()).set_node_wrapper(node_wrapper)
            out = converter.convert()
            code_str += out + "\n"
            if nodes:
                log_info: Dict = {"node_id": nodes.get(node_wrapper.get_node()), "status": "compile_succ"}
                code_str += "print('#LOG#NODEINFO#" + json.dumps(log_info) + "')\n"

        self.converted_code = code_str
        return code_str

    def run_code(self, node_id: int, code: str, output_type: str = 'console', node_mode: str = 'dataframe', *args,
                 **kwargs) -> Any:
        self.execution_handler.executor = cast(Executor, self.execution_handler.executor)
        # print(code)
        try:
            self.execution_handler.executor.join()
        except RuntimeError:
            pass
        self.execution_handler.create_executor()
        if output_type == 'schema':
            schema = Schema()
            # print(schema)
            self.execution_handler.executor.set_output_info(output_type=output_type, output=schema)
            self.load_code(code)
            self.execution_handler.executor.start()
            self.execution_handler.executor.join()
            return schema
        if output_type == 'console':
            output: Dict = {}
            if node_mode == 'estimator':
                self.execution_handler.executor.cache_path = f'{node_id}_cache'
            self.execution_handler.executor.set_output_info(output_type=output_type, output=output)
            self.load_code(code)
            self.execution_handler.executor.start()
            self.execution_handler.executor.join()
            return output

    def model_run_code(self) -> str:
        return "import pandas\nimport numpy as np\nimport json\n" \
               + self.converted_code + "\n"

    def runnable_code(self, mode: 'str' = 'dataframe') -> str:
        if mode == 'dataframe':
            return '''from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
spark = SparkSession.builder\\
                    .appName('SparkByExamples.com') \\
                    .getOrCreate()\n
def spark_concat(df1, df2):
    schema = StructType(df1.schema.fields + df2.schema.fields)
    df1df2 = df1.rdd.zip(df2.rdd).map(lambda x: x[0]+x[1])
    return spark.createDataFrame(df1df2, schema)\n
    
''' + \
                   self.converted_code + '\n' + \
                   "ident0.show(truncate = False)"
        elif mode == 'estimator' or mode == 'write':
            return '''from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
spark = SparkSession.builder\\
                    .appName('SparkByExamples.com') \\
                    .getOrCreate()\n
def spark_concat(df1, df2):
    schema = StructType(df1.schema.fields + df2.schema.fields)
    df1df2 = df1.rdd.zip(df2.rdd).map(lambda x: x[0]+x[1])
    return spark.createDataFrame(df1df2, schema)\n
    
''' + \
                   self.converted_code + '\n' + \
                   "print(ident0)"
        elif mode == 'metrics':
            return '''from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
import json
spark = SparkSession.builder\\
                    .appName('SparkByExamples.com') \\
                    .getOrCreate()\n
def spark_concat(df1, df2):
    schema = StructType(df1.schema.fields + df2.schema.fields)
    df1df2 = df1.rdd.zip(df2.rdd).map(lambda x: x[0]+x[1])
    return spark.createDataFrame(df1df2, schema)\n
    
''' + \
                   self.converted_code + '\n' + \
                   'print(json.dumps(ident0, indent=4))'

    def schema_code(self) -> str:
        return
