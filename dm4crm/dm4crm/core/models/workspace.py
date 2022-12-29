from typing import List, Optional, cast, Any, Dict, Tuple

from .dataflow import Dataflow
from .engine.baseengine.base_engine import BaseEngine
from .node import *
from .engine.pandasengine.pandas_engine import PandasEngine
from .engine.sparkengine.spark_engine import SparkEngine
from .execution.pandas_local_execution_handler import PandasLocalExecutionHandler
from .execution.spark_local_execution_handler import SparkLocalExecutionHandler
from .schema.schema import Schema
from .utils import camel_case_split


class Workspace:
    ws = None

    def __init__(self, engine_type: str = '') -> None:
        self.nodes: Dict = {}
        self.nodes_ui_pos: Dict = {}
        self.engine_type: str = engine_type
        self.crm_info = {"host": None, "port": None, "user": None, "database": None, "password": None}
        self.engine: Optional[BaseEngine] = None
        self.connections: List = []
        self.save_ws_file = ""
        self.new_node_id = 0
        self.io_nodes = {"CSVReader": CSVReaderNode,
                         "CSVWriter": CSVWriterNode,
                         "MysqlReader": MysqlReaderNode}
        self.transform_nodes = {"Limit": LimitNode,
                                "Shuffle": ShuffleRowsNode,
                                "ConstantColumn": ConstantColumnNode,
                                "ChangeColumnType": ChangeColumnTypeNode,
                                "SelectColumn": SelectColumnNode,
                                "Sort": SortNode,
                                "SplitColumn": SplitColumnNode,
                                "Groupby": GroupbyNode,
                                "IsMissing": IsMissingNode,
                                "FillMissing": FillMissingNode,
                                "Join": JoinNode,
                                "Union": UnionNode,
                                "CaseWhen": CaseWhenNode,
                                "Where": WhereNode,
                                "SeparateTargetColumn": SeparateTargetColumnNode,
                                "DropColumn": DropColumnNode,
                                "TrainTestSplit": TrainTestSplitNode,
                                "Concat": ConcatNode,
                                "Duplicate": DuplicateNode,
                                "CategoryToNum": CategoryToNumNode,
                                "ToNumeric": ToNumericNode,
                                "DropNa": DropNaNode
                                }
        self.model_learner_nodes = {"LogisticRegression": LogisticRegressionNode,
                                    "DecisionTreeClassifier": DecisionTreeClassifierNode,
                                    "RandomForestClassifier": RandomForestClassifierNode,
                                    "NeuralNetworkClassifier": NeuralNetworkClassifierNode}
        self.metrics_node = {"Score": ScoreNode}
        self.crm_nodes = {"AccountsCrm": AccountsCrmNode,
                          "CampaignsCrm": CampaignsCrmNode
                          }
        self.data_mining_nodes = {**self.model_learner_nodes, **self.metrics_node, "Predict": PredictNode}
        self.available_nodes = {**self.io_nodes,
                                **self.transform_nodes,
                                **self.data_mining_nodes,
                                **self.crm_nodes}
        self.set_attributes(crm_info={"host": "192.168.0.164",
                                      "port": "3307",
                                      "user": "bn_suitecrm",
                                      "database": "bitnami_suitecrm",
                                      "password": "bitnami123"})

    @staticmethod
    def get_workspace():
        if Workspace.ws:
            return Workspace.ws
        Workspace.ws = Workspace()
        return Workspace.ws

    def reset_workspace(self):
        Workspace.ws = None
        Workspace.get_workspace()

    def set_attributes(self, *args, **kwargs):
        self.engine_type = kwargs.get("engine_type", self.engine_type)
        print(self.engine_type)
        self.crm_info = kwargs.get("crm_info", self.crm_info)
        self.set_save_file(kwargs.get("save_ws_file", self.save_ws_file))
        if self.engine_type == 'pandas':
            self.engine = None
            self.new_engine()
            self.engine = cast(PandasEngine, self.engine)
            self.engine.set_run_env(kwargs.get("run_env", self.engine.execution_handler.executor.run_env))
            self.engine.set_temp_dir(kwargs.get("temp_dir", self.engine.execution_handler.executor.temp_dir))
            self.new_engine()
        elif self.engine_type == 'spark':
            self.engine = None
            self.new_engine()
            self.engine = cast(SparkEngine, self.engine)
            self.engine.set_spark_home(kwargs.get("run_env", self.engine.execution_handler.executor.spark_home))
            self.engine.set_temp_dir(kwargs.get("temp_dir", self.engine.execution_handler.executor.temp_dir))
        print(self.engine_type)

    def set_save_file(self, filename: str) -> None:
        if not filename.endswith('.ws'):
            filename += '.ws'
        self.save_ws_file = filename

    def get_workspace_info(self):
        dic: Dict = {}
        dic["engine_type"] = self.engine_type
        dic["nodes"] = self.nodes
        dic["crm_info"] = self.crm_info
        dic["nodes_ui_pos"] = self.nodes_ui_pos
        dic["connections"] = self.connections
        dic["new_node_id"] = self.new_node_id
        if self.engine_type == 'pandas':
            d = {}
            self.engine.execution_handler = cast(PandasLocalExecutionHandler, self.engine.execution_handler)
            d["run_env"] = self.engine.execution_handler.executor.run_env
            d["temp_dir"] = self.engine.execution_handler.executor.temp_dir
            dic["engine"] = d
        elif self.engine_type == 'spark':
            d = {}
            self.engine.execution_handler = cast(SparkLocalExecutionHandler, self.engine.execution_handler)
            print(self.engine.execution_handler.executor.spark_home)
            d["run_env"] = self.engine.execution_handler.executor.spark_home
            d["temp_dir"] = self.engine.execution_handler.executor.temp_dir
            dic["engine"] = d
        return dic

    def save_workspace(self):
        import pickle
        with open(self.save_ws_file, "wb") as f:
            dic: Dict = self.get_workspace_info()
            pickle.dump(dic, f)

    @staticmethod
    def load_workspace(save_ws_file: str):
        import pickle
        with open(save_ws_file, "rb") as f:
            dic = pickle.load(f)
            Workspace.ws = Workspace()
            Workspace.ws.engine_type = dic["engine_type"]
            Workspace.ws.nodes = dic["nodes"]
            print(Workspace.ws.nodes)
            Workspace.ws.crm_info = dic["crm_info"]
            Workspace.ws.nodes_ui_pos = dic["nodes_ui_pos"]
            Workspace.ws.connections = dic["connections"]
            print(Workspace.ws.connections)
            Workspace.ws.save_ws_file = save_ws_file
            for origin_node_id, dest_node_id, origin_node_port, dest_node_port in Workspace.ws.connections:
                if Workspace.ws.nodes[origin_node_id] and Workspace.ws.nodes[dest_node_id]:
                    Workspace.ws.connect_nodes(origin_node_id, dest_node_id, origin_node_port, dest_node_port)
            Workspace.ws.new_node_id = dic["new_node_id"]
            if Workspace.ws.engine_type == 'pandas':
                pe: PandasEngine = PandasEngine()
                pe.execution_handler = PandasLocalExecutionHandler()
                pe.set_run_env(dic["engine"]["run_env"])
                pe.set_temp_dir(dic["engine"]["temp_dir"])
                pe.execution_handler.create_executor()
                Workspace.ws.engine = cast(PandasEngine, Workspace.ws.engine)
                Workspace.ws.engine = pe
            elif Workspace.ws.engine_type == 'spark':
                se: SparkEngine = SparkEngine()
                se.execution_handler = SparkLocalExecutionHandler()
                se.set_spark_home(dic["engine"]["run_env"])
                se.set_temp_dir(dic["engine"]["temp_dir"])
                se.execution_handler.create_executor()
                Workspace.ws.engine = cast(SparkEngine, Workspace.ws.engine)
                Workspace.ws.engine = se
            print(dic["engine"]["run_env"])

    def get_nodes(self):
        return self.nodes

    def set_node_ui_pos(self, node_id, pos_x, pos_y):
        self.nodes_ui_pos[node_id] = (pos_x, pos_y)

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

    def get_available_nodes_categorized(self) -> Dict:
        return {
            "CRM": list(self.crm_nodes.keys()),
            "Input/Output": list(self.io_nodes.keys()),
            "Transform": list(self.transform_nodes.keys()),
            "Data Mining": list([*self.model_learner_nodes.keys(),
                                 *self.metrics_node.keys(),
                                 "Predict"])
        }

    def get_nodes_nodes_id(self) -> Dict:
        dic = {}
        for node_id, node in self.nodes.items():
            dic[node] = node_id
        return dic

    def copy_node(self, node_id):
        print('__dict__' in dir(self.nodes[node_id]), self.nodes[node_id])

    def create_node(self, node_type: str, **kwargs) -> int:
        new_node: GeneralNode
        if node_type in self.available_nodes:
            if node_type in self.crm_nodes:
                kwargs = {**self.crm_info}
            else:
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
        self.connections.remove((from_node_id, dest_node_id, from_port, dest_port))
        dest_node: NonInitialNode = cast(NonInitialNode, self.nodes[dest_node_id])
        self.nodes[from_node_id].remove_connected_node(dest_node, from_port, dest_port)

    def remove_node(self, node_id: int):
        node = self.nodes[node_id]
        if isinstance(node, NonInitialNode):
            for from_node in node.get_in_ports().values():
                if from_node:
                    from_node.full_remove_connected_nodes(node)
        for dest_node in node.get_out_ports().values():
            if dest_node:
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
        elif self.engine_type == 'spark':
            se: SparkEngine = SparkEngine()
            se.execution_handler = SparkLocalExecutionHandler()
            se.set_spark_home(r"C:\Users\mhset\OneDrive\Desktop\Project_ws\spark_home\spark-3.2.0-bin-hadoop3.2")
            se.set_temp_dir(r"C:\Users\mhset\OneDrive\Desktop\Project_ws\temp")
            se.execution_handler.create_executor()
            self.engine = cast(SparkEngine, self.engine)
            self.engine = se
        return None

    def compile(self, node_id: int) -> None:
        if not self.engine:
            raise Exception("Engine is not initiated")
        df: Dataflow = Dataflow()
        df.target_node = self.nodes[node_id]
        df.set_node_connections([(self.nodes[f_node], self.nodes[d_node], f_port, d_port)
                                 for f_node, d_node, f_port, d_port in self.connections])
        df.refresh()
        self.engine.dataflow = df
        self.engine.convert_code(nodes=self.get_nodes_nodes_id())
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
        if type(self.engine.dataflow.target_node) in self.transform_nodes.values() or \
                type(self.engine.dataflow.target_node) == PredictNode or \
                type(self.engine.dataflow.target_node) == CSVReaderNode or \
                type(self.engine.dataflow.target_node) == MysqlReaderNode or \
                type(self.engine.dataflow.target_node) in self.crm_nodes.values():
            mode = 'dataframe'
        elif type(self.engine.dataflow.target_node) in self.model_learner_nodes.values():
            mode = 'estimator'
        elif type(self.engine.dataflow.target_node) == CSVWriterNode:
            mode = 'write'
        elif type(self.engine.dataflow.target_node) in self.metrics_node.values():
            mode = 'metrics'
        code: str = self.engine.runnable_code(mode)
        return self.run_code(code, 'console', node_mode=mode)
