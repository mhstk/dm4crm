from abc import ABC, abstractmethod
from threading import Thread
from typing import Any


class Executor(ABC, Thread):

    def __init__(self):
        super().__init__()
        self.code: str = ''
        self.run_env: str = ''
        self.temp_dir: str = ''
        self.run_type: str = ''
        self.output_type: str = ''
        self.output_name: str = ''
        self.output: Any = None
        self.cache_path = ''

    @abstractmethod
    def set_output_info(self, output_type: str, *args, **kwargs):
        pass


