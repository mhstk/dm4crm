from .local_execution_handler import LocalExecutionHandler
from .pandas_executor import PandasExecutor


class PandasLocalExecutionHandler(LocalExecutionHandler):

    def __init__(self):
        super(PandasLocalExecutionHandler, self).__init__()

    def create_executor(self):
        self.executor = PandasExecutor(run_env=self.run_env, temp_dir=self.temp_dir)



