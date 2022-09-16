from ..initial_node import InitialNode


class MysqlReaderNode(InitialNode):
    __slots__ = 'host', 'port', 'database', 'user', 'password', 'table'

    def __init__(self, host: str = '', port: str = '3306', database: str = '',
                 user: str = '', password: str = '', table: str = ''):
        super(MysqlReaderNode, self).__init__()
        self.host = host
        self.port = port
        self.user = user
        self.database = database
        self.password = password
        self.table = table

    def set_attribute(self, *args, **kwargs):
        pass
