from abc import ABC, abstractmethod
from typing import Optional, Any, Dict

from .converter import Converter
from ...dataflow import Dataflow
from ...execution.execution_handler import ExecutionHandler
from ...node.general_node import GeneralNode


class BaseEngine(ABC):
    __slots__ = "_dataflow", "_execution_handler", "_converted_code", "_node_converter_map"

    def __init__(self) -> None:
        self._dataflow: Optional[Dataflow] = None
        self._execution_handler: Optional[ExecutionHandler] = None
        self._converted_code: str = ''
        self._node_converter_map: Dict = {}

    def load_code(self, code: str):
        self.execution_handler.update_code(code)

    @property
    def node_converter_map(self):
        return self._node_converter_map

    @node_converter_map.setter
    def node_converter_map(self, value):
        self._node_converter_map = value

    @property
    def converted_code(self):
        return self._converted_code

    @converted_code.setter
    def converted_code(self, value: str):
        self._converted_code = value

    @property
    def execution_handler(self):
        return self._execution_handler

    @execution_handler.setter
    def execution_handler(self, value: ExecutionHandler):
        self._execution_handler = value

    @property
    def dataflow(self):
        return self._dataflow

    @dataflow.setter
    def dataflow(self, df: Dataflow):
        self._dataflow = df

    @abstractmethod
    def dispatcher(self, node: GeneralNode) -> Converter:
        pass

    @abstractmethod
    def convert_code(self, nodes: Optional[Dict] = None) -> str:
        pass

    @abstractmethod
    def run_code(self, node_id: int, code: str, node_mode: str, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    def model_run_code(self) -> str:
        pass

    @abstractmethod
    def runnable_code(self, mode: str) -> str:
        pass

    @abstractmethod
    def schema_code(self) -> str:
        pass

