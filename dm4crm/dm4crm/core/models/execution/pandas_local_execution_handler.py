from .local_execution_handler import LocalExecutionHandler
from .pandas_executor import PandasExecutor


class PandasLocalExecutionHandler(LocalExecutionHandler):

    def __init__(self, run_env: str = ''):
        super(PandasLocalExecutionHandler, self).__init__()
        self.run_env = run_env

    def create_executor(self):
        self.executor = PandasExecutor(run_env=self.run_env, temp_dir=self.temp_dir)



