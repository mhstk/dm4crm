class Column:
    def __init__(self, name: str = '', type: str = '', is_nullable: bool = True, num_of_nulls: int = 0):
        self.name: str = name
        self.type: str = type
        self.is_nullable: bool = is_nullable
        self.num_of_nulls: int = num_of_nulls

    def __repr__(self):
        return str((self.name, self.type))
