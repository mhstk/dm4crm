import os
import subprocess
import sys
from typing import Dict, cast, Any

from .executor import Executor
from ..utils import *
from ..schema.column import Column
from ..schema.schema import Schema


class SparkExecutor(Executor):
    def __init__(self, spark_home: str, temp_dir: str):
        super(SparkExecutor, self).__init__()
        self.spark_home = spark_home
        self.temp_dir = temp_dir
        self.run_type = "temp_run_code"
        self.output_type = "console"
        self.output_name = "test3.py"
        self.temp_code = os.path.join(self.temp_dir, self.run_type)

    def set_output_info(
            self, output_type: str = "console", run_type: str = "temp_run_code",
            output_name: str = "test3.py", output: Any = None, *args, **kwargs):
        self.output_type = output_type
        self.run_type = run_type
        self.output_name = output_name
        self.temp_code = os.path.join(self.temp_dir, self.run_type)
        self.output = output

    def save_code(self):
        file_path: str = os.path.join(self.temp_code, self.output_name)
        os.makedirs(self.temp_code, exist_ok=True)
        with open(file_path, "w") as f:
            f.write(self.code)

    def run_code(self):


        # code_path: str = os.path.join(BASE_DIR, "core", "OutTemp", "test.py")
        code_path: str = os.path.join(self.temp_code, self.output_name)
        spark_submit_path: str = os.path.join(self.spark_home, "bin", "spark-submit")
        if sys.platform.startswith('win'):
            spark_submit_path += ".cmd"
        # try:
        #     output: str = subprocess.check_output([spark_submit_path, code_path], shell=True, stderr=subprocess.STDOUT)
        # except subprocess.CalledProcessError as e:
        #     raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
        print(f'{spark_submit_path} {code_path}')
        process: subprocess.Popen[bytes] = subprocess.Popen([spark_submit_path, code_path, "--master local[*]"],
                                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        output, error = process.communicate()

        print("DONE\n\n-----------------------")
        output = output.decode('utf8')
        print(output)
        if error:
            for line in error.decode('utf8').splitlines():
                if "warning" not in line.lower():
                    output += "\nError: \n" + line
        return output

        # for line in process.stdout:
        #     print(line.decode('utf8'), end="")

    def output_func(self, out_str: str):
        if self.output_type == 'console':
            self.output = cast(Dict, self.output)
            print("EXECUTED:\n")
            # print(out_str)
            out = output_parse(out_str)
            self.output.update(out)
            # dic = json.loads(out_str)
            # self.output.update(dic)
        elif self.output_type == 'schema':
            self.output = cast(Schema, self.output)
            # print(out_str)
            for line in out_str.splitlines():
                if line and not line.startswith("DEL#") and not line.startswith("#LOG#"):
                    col, col_type = line.split(" ")
                    new_col = Column(name=col, type=col_type)
                    self.output.columns.append(new_col)
                if line.startswith("DEL##"):
                    break

    def run(self) -> None:
        self.save_code()
        out_str: str = self.run_code()
        self.output_func(out_str)


