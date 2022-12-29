from .local_execution_handler import LocalExecutionHandler
from .spark_executor import SparkExecutor


class SparkLocalExecutionHandler(LocalExecutionHandler):

    def __init__(self, spark_home: str = ''):
        super(SparkLocalExecutionHandler, self).__init__()
        self.spark_home = spark_home

    def create_executor(self):
        self.executor = SparkExecutor(spark_home=self.spark_home, temp_dir=self.temp_dir)



