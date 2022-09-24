import json
from typing import cast, Any, Dict, Optional

from .converters import *
from ...execution.executor import Executor
from ...node import *
from ..baseengine.base_engine import BaseEngine
from ..baseengine.converter import Converter
from ...execution.pandas_local_execution_handler import PandasLocalExecutionHandler
from ...schema.schema import Schema


class PandasEngine(BaseEngine):

    def __init__(self) -> None:
        super().__init__()
        self.execution_handler: PandasLocalExecutionHandler = cast(PandasLocalExecutionHandler, self.execution_handler)
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
                                         PredictNode: PredictConverter}

    def set_run_env(self, run_env: str):
        self.execution_handler.run_env = run_env

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
            return "import pandas\nimport numpy as np\nimport json\n" \
                   "pandas.set_option('display.max_rows', 500)\n" \
                   "pandas.set_option('display.max_columns', 500)\n" \
                   "pandas.set_option('display.width', 1000)\n" \
                   + self.converted_code + "\n" \
                   + "result = ident0.iloc[0:20]\n" \
                   + "print(result)\nprint('#LOG#ENDRUNNIG#')\n"
                   # + "result = ident0.iloc[0:20].to_json(orient='table')\n" \
                   # + "parsed = json.loads(result)\n" \
                   # + "print(json.dumps(parsed, indent=4))"
        elif mode == 'estimator' or mode == 'write':
            return "import pandas\nimport numpy as np\nimport json\n" \
                   + self.converted_code + "\n" \
                   + 'print("{\\"message\\" : \\"Success\\"}")'
        # elif mode == 'predict':
        #     return "import pandas\nimport numpy as np\nimport json\n" \
        #            + self.converted_code + "\n" \
        #            + 'result = {"out" :  ident0.values.tolist()}\n' \
        #            + 'print(json.dumps(result, indent=4))'
        elif mode == 'metrics':
            return "import pandas\nimport numpy as np\nimport json\n" \
                   + self.converted_code + "\n" \
                   + 'print(json.dumps(ident0, indent=4))'

    def schema_code(self) -> str:
        return "import pandas\n" \
               + self.converted_code + "\n" \
               + '''
for index, value in ident0.dtypes.items():
    print(index, value)
print("DEL####DEL")
for index, value in ident0.isnull().sum().items():
    print(index, value)\n'''
