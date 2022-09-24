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
        self.host = kwargs.get("host", self.host)
        self.port = kwargs.get("port", self.port)
        self.database = kwargs.get("database", self.database)
        self.user = kwargs.get("user", self.user)
        self.password = kwargs.get("password", self.password)
        self.table = kwargs.get("table", self.table)







