from typing import List, Optional, cast, Any, Dict, Tuple

from .dataflow import Dataflow
from .engine.baseengine.base_engine import BaseEngine
from .node import *
from .engine.pandasengine.pandas_engine import PandasEngine
from .execution.pandas_local_execution_handler import PandasLocalExecutionHandler
from .schema.schema import Schema


class Workspace:
    ws = None

    def __init__(self, engine_type: str = '') -> None:
        self.nodes: Dict = {}
        self.engine_type: str = engine_type
        self.engine: Optional[BaseEngine] = None
        self.available_nodes: Dict = {}
        self.connections: List = []
        self.new_node_id = 0
        self.transform_nodes = {"CSVReader": CSVReaderNode, "Limit": LimitNode, "Shuffle": ShuffleRowsNode,
                                "ConstantColumn": ConstantColumnNode, "ChangeColumnType": ChangeColumnTypeNode,
                                "SelectColumn": SelectColumnNode, "Sort": SortNode,
                                "SplitColumn": SplitColumnNode, "Groupby": GroupbyNode,
                                "IsMissing": IsMissingNode, "FillMissing": FillMissingNode, "Join": JoinNode,
                                "Union": UnionNode, "CaseWhen": CaseWhenNode, "Where": WhereNode,
                                "SeparateTargetColumn": SeparateTargetColumnNode, "DropColumn": DropColumnNode,
                                "TrainTestSplit": TrainTestSplitNode, "Concat": ConcatNode, "Duplicate": DuplicateNode}
        self.model_learner_nodes = {"LogisticRegression": LogisticRegressionNode}
        self.data_mining_nodes = {**self.model_learner_nodes, "Predict": PredictNode}

    @staticmethod
    def get_workspace():
        if Workspace.ws:
            return Workspace.ws
        Workspace.ws = Workspace()
        Workspace.ws.available_nodes = {**Workspace.ws.transform_nodes, **Workspace.ws.data_mining_nodes}
        return Workspace.ws

    def get_nodes(self):
        return self.nodes

    def get_connected_nodes(self):
        return self.connections

    def get_node_id_by_node_obj(self, node: GeneralNode):
        for node_id, node_obj in self.nodes.items():
            if node == node_obj:
                return node_id
        return None

    def get_connection_by_from_node_id_port(self, node_id: int, port: int) -> Optional[Tuple]:
        for connection in self.connections:
            if connection[0] == node_id and connection[2] == port:
                return connection
        return None

    def get_available_nodes(self):
        return self.available_nodes

    def full_duplicate_node(self, node_id):
        # TODO
        pass

    def create_node(self, node_type: str, **kwargs) -> int:
        new_node: GeneralNode
        if node_type in self.available_nodes:
            kwargs = {key: kwargs[key] for key in kwargs.keys() if key in self.available_nodes[node_type].__slots__}
            new_node = self.available_nodes[node_type](**kwargs)
        else:
            raise Exception("There is no node with this type")

        if new_node:
            self.nodes[self.new_node_id] = new_node
            self.new_node_id += 1
            return self.new_node_id - 1
        return -1

    def connect_nodes(self, from_node_id: int, dest_node_id: int, from_port: int = 0, dest_port: int = 0):
        from_node: GeneralNode = cast(GeneralNode, self.nodes[from_node_id])
        old_connection: Optional[Tuple] = self.get_connection_by_from_node_id_port(from_node_id, from_port)
        if old_connection:
            old_dest_node: NonInitialNode = self.nodes[old_connection[1]]
            old_dest_port: int = old_connection[3]
            from_node.remove_connected_node(old_dest_node, from_port, old_dest_port)
            self.connections.remove(old_connection)

        dest_node: NonInitialNode = cast(NonInitialNode, self.nodes[dest_node_id])
        from_node.connect_to(dest_node, from_port=from_port, dest_port=dest_port)
        self.connections.append((from_node_id, dest_node_id, from_port, dest_port))

    def disconnect_nodes(self, from_node_id: int, dest_node_id: int, from_port: int = 0, dest_port: int = 0):
        dest_node: NonInitialNode = cast(NonInitialNode, self.nodes[dest_node_id])
        self.nodes[from_node_id].remove_connected_node(dest_node, from_port, dest_port)

    def remove_node(self, node_id: int):
        node = self.nodes[node_id]
        if isinstance(node, NonInitialNode):
            for from_node in node.get_in_ports().values():
                from_node.full_remove_connected_nodes(node)
        for dest_node in node.get_out_ports().values():
            node.full_remove_connected_nodes(dest_node)
        for connection in self.connections:
            if connection[0] == node_id or connection[1] == node_id:
                self.connections.remove(connection)
        self.nodes[node_id] = None
        return True

    def new_engine(self) -> None:
        if self.engine:
            return
        if self.engine_type == 'pandas':
            pe: PandasEngine = PandasEngine()
            pe.execution_handler = PandasLocalExecutionHandler()
            pe.set_run_env(r"C:\Users\mhset\OneDrive\Desktop\Project_ws\pandas_exec\venv\Scripts")
            pe.set_temp_dir(r"C:\Users\mhset\OneDrive\Desktop\Project_ws\temp")
            pe.execution_handler.create_executor()
            self.engine = cast(PandasEngine, self.engine)
            self.engine = pe
        return None

    def compile(self, node_id: int) -> None:
        if not self.engine:
            raise Exception("Engine is not initiated")
        df: Dataflow = Dataflow()
        df.target_node = self.nodes[node_id]
        df.refresh()
        self.engine.dataflow = df
        self.engine.convert_code()
        return

    def run_code(self, code: str, output_type: str = 'console', node_mode: str = 'dataframe') -> Any:
        self.engine = cast(BaseEngine, self.engine)
        node_id: int = self.get_node_id_by_node_obj(self.engine.dataflow.target_node)
        return self.engine.run_code(node_id, code, output_type, node_mode)

    def get_schema(self) -> Schema:
        self.engine = cast(BaseEngine, self.engine)
        code: str = self.engine.schema_code()
        return self.run_code(code, 'schema')

    def show(self):
        self.engine = cast(BaseEngine, self.engine)
        mode = ''
        if type(self.engine.dataflow.target_node) in self.transform_nodes.values():
            mode = 'dataframe'
        elif type(self.engine.dataflow.target_node) in self.model_learner_nodes.values():
            mode = 'estimator'
        elif type(self.engine.dataflow.target_node) == PredictNode:
            mode = 'predict'
        code: str = self.engine.runnable_code(mode)
        return self.run_code(code, 'console', node_mode=mode)
