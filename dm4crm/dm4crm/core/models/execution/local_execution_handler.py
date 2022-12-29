from abc import ABCMeta

from .execution_handler import ExecutionHandler


class LocalExecutionHandler(ExecutionHandler, metaclass=ABCMeta):
    def __init__(self, temp_dir: str = ""):
        super(LocalExecutionHandler, self).__init__()
        self.temp_dir = temp_dir


