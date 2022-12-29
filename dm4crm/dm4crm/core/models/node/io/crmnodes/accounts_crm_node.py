from ..mysql_reader_node import MysqlReaderNode


class AccountsCrmNode(MysqlReaderNode):

    def __init__(self, host: str = '', port: str = '', database: str = '',
                 user: str = '', password: str = ''):
        super(AccountsCrmNode, self).__init__(host=host, port=port, database=database, user=user, password=password, table='')
        self.table = 'accounts'
