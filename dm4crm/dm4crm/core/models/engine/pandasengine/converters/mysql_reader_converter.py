from ...baseengine.converter import Converter
from ....node.io.mysql_reader_node import MysqlReaderNode


class MysqlReaderConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: MysqlReaderNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        code_str = ""

        code_str += f'import mysql.connector as connection\n'
        code_str += f'mydb = None\n'
        code_str += f'''try:
    mydb = connection.connect(host="{node.host}", port={node.port},\
    database="{node.database}",user="{node.user}", passwd="{node.password}",use_pure=True)
    query = "Select * from {node.table};"
    {out_port_ident} = pandas.read_sql(query,mydb)
    mydb.close() #close the connection
except Exception as e:
    if mydb:
        mydb.close()
    print(str(e))
    exit()
        '''



        return code_str

