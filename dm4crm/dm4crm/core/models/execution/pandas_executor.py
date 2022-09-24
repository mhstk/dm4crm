import os
import subprocess
import sys
from typing import Dict, cast, Any

from .executor import Executor
from ..utils import *
from ..schema.column import Column
from ..schema.schema import Schema


class PandasExecutor(Executor):
    def __init__(self, run_env: str, temp_dir: str):
        super(PandasExecutor, self).__init__()
        self.run_env = run_env
        self.temp_dir = temp_dir
        self.run_type = "temp_run_code"
        self.output_type = "console"
        self.output_name = "test.py"
        self.temp_code = os.path.join(self.temp_dir, self.run_type)

    def set_output_info(
            self, output_type: str = "console", run_type: str = "temp_run_code",
            output_name: str = "test.py", output: Any = None, *args, **kwargs):
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

        my_env: Dict = os.environ.copy()
        # my_env["PATH"] = os.path.join(BASE_DIR.parent, "venv", "Scripts") + os.pathsep + my_env["PATH"]
        my_env["PATH"] = self.run_env + os.pathsep + my_env["PATH"]
        # code_path: str = os.path.join(BASE_DIR, "core", "OutTemp", "test.py")
        code_path: str = os.path.join(self.temp_code, self.output_name)
        python_path: str = os.path.join(self.run_env, "python")
        if sys.platform.startswith('win'):
            python_path += ".exe"
        print("Running code...")
        # output: str = subprocess.check_output([python_path, code_path], env=my_env)
        process: subprocess.Popen[bytes] = subprocess.Popen([python_path, code_path], env=my_env,
                                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # out_data = ''
        # while '#LOG#ENDRUNNIG#' not in out_data:
        #     out_data = process.stdout.readline().decode('utf8')
        #     if "#LOG#NODEINFO" in out_data:
        #         print(out_data)
        output, error = process.communicate()

        print("DONE\n\n-----------------------")
        output = output.decode('utf8')
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


