from ....node.io.csv_reader_node import CSVReaderNode
from ...baseengine.converter import Converter


class CSVReaderConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: CSVReaderNode = self.get_node_wrapper().get_node()
        out_port_ident: str = self.get_node_wrapper().get_out_idents()[0]
        code_str = ""
        if node.file_path:
            code_str += f'{out_port_ident} = spark.read\\\n\
    .format("csv")\\\n\
    .option("header", "true")\\\n\
    .option("encoding", "UTF-8")\\\n\
    .option("inferSchema", True)\\\n\
    .load(r"{node.file_path}")'
        return code_str

