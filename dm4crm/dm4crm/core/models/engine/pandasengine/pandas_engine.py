from typing import cast, Any, Dict

from .converters import *
from ...execution.executor import Executor
from ...node import *
from ..baseengine.base_engine import BaseEngine
from ..baseengine.converter import Converter
from ...execution.pandas_local_execution_handler import PandasLocalExecutionHandler
from ...schema.schema import Schema


class PandasEngine(BaseEngine):
    __slots__ = "_dataflow", "_execution_handler", "_converted_code"

    def __init__(self) -> None:
        super().__init__()
        self.execution_handler: PandasLocalExecutionHandler = cast(PandasLocalExecutionHandler, self.execution_handler)

    def set_run_env(self, run_env: str):
        self.execution_handler.run_env = run_env

    def set_temp_dir(self, temp_dir: str):
        self.execution_handler.temp_dir = temp_dir

    def dispatcher(self, node: GeneralNode) -> Converter:
        if isinstance(node, LimitNode):
            return LimitConverter()
        elif isinstance(node, CSVReaderNode):
            return CSVReaderConverter()
        elif isinstance(node, SelectColumnNode):
            return SelectColumnConverter()
        elif isinstance(node, ChangeColumnTypeNode):
            return ChangeColumnTypeConverter()
        elif isinstance(node, SortNode):
            return SortConverter()
        elif isinstance(node, ShuffleRowsNode):
            return ShuffleRowsConverter()
        elif isinstance(node, SplitColumnNode):
            return SplitColumnConverter()
        elif isinstance(node, ConstantColumnNode):
            return ConstantColumnConverter()
        elif isinstance(node, GroupbyNode):
            return GroupbyConverter()
        elif isinstance(node, IsMissingNode):
            return IsMissingConverter()
        elif isinstance(node, FillMissingNode):
            return FillMissingConverter()
        elif isinstance(node, JoinNode):
            return JoinConverter()
        elif isinstance(node, UnionNode):
            return UnionConverter()
        elif isinstance(node, CaseWhenNode):
            return CaseWhenConverter()
        elif isinstance(node, WhereNode):
            return WhereConverter()
        elif isinstance(node, SeparateTargetColumnNode):
            return SeparateTargetColumnConverter()
        elif isinstance(node, DropColumnNode):
            return DropColumnConverter()
        elif isinstance(node, TrainTestSplitNode):
            return TrainTestSplitConverter()
        elif isinstance(node, LogisticRegressionNode):
            return LogisticRegressionConverter()
        elif isinstance(node, PredictNode):
            return PredictConverter()
        elif isinstance(node, ConcatNode):
            return ConcatConverter()
        elif isinstance(node, DuplicateNode):
            return DuplicateConverter()
        elif isinstance(node, DecisionTreeClassifierNode):
            return DecisionTreeClassifierConverter()
        elif isinstance(node, ScoreNode):
            return ScoreConverter()
        else:
            raise Exception(f"Convertor for this node wasn't found: {node}")

    def convert_code(self) -> str:
        code_str: str = ""
        for node_wrapper in self.dataflow.wrap_nodes:
            converter: Converter = self.dispatcher(node_wrapper.get_node()).set_node_wrapper(node_wrapper)
            out = converter.convert()
            code_str += out + "\n"

        self.converted_code = code_str
        return code_str

    def run_code(self, node_id: int, code: str, output_type: str = 'console', node_mode: str = 'dataframe', *args, **kwargs) -> Any:
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
                   + self.converted_code + "\n" \
                   + "result = ident0.iloc[0:20].to_json(orient='table')\n" \
                   + "parsed = json.loads(result)\n" \
                   + "print(json.dumps(parsed, indent=4))"
        elif mode == 'estimator':
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
