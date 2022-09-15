from abc import ABC, abstractmethod
from typing import Optional

from .executor import Executor


class ExecutionHandler(ABC):
    def __init__(self):
        self.executor: Optional[Executor] = None

    def update_code(self, new_code: str):
        if self.executor:
            self.executor.code = new_code

    @abstractmethod
    def create_executor(self):
        pass
