import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def create_node(name, in_type="transform"):
    path = ""
    node_class_name = get_class_name(name) + "Node"
    node_module_name = name + "_node"
    if in_type.lower() == 'transform':
        path = os.path.join(BASE_DIR, "models", "Node", "Transform")
    elif in_type.lower() == 'datamining':
        path = os.path.join(BASE_DIR, "models", "Node", "datamining")
    if path:
        with open(os.path.join(path, node_module_name+".py"), 'w') as f:
            s = f'''from ..non_initial_node import NonInitialNode


class {node_class_name}(NonInitialNode):
    __slots__ = ''

    def __init__(self):
        super({node_class_name}, self).__init__()
'''
            f.write(s)
        with open(os.path.join(path, "__init__.py"), 'a') as f:
            s = f"from .{node_module_name} import {node_class_name}\n"
            f.write(s)


def create_converter(name, engine="pandas", node_type='transform'):
    path = ""
    converter_class_name = get_class_name(name) + "Converter"
    converter_module_name = name+"_converter"
    node_class_name = get_class_name(name) + "Node"
    node_module_name = name + "_node"
    if engine.lower() == "pandas":
        path = os.path.join(BASE_DIR, "models", "Engine", "PandasEngine", "Converters")

    if path:
        with open(os.path.join(path, converter_module_name+".py"), 'w') as f:
            s = f'''from ...baseengine.converter import Converter
from ....node.{node_type.lower()}.{node_module_name} import {node_class_name}


class {converter_class_name}(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: {node_class_name} = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        code_str = ""


        return code_str

'''
            f.write(s)
        with open(os.path.join(path, "__init__.py"), 'a') as f:
            s = f"from .{converter_module_name} import {converter_class_name}\n"
            f.write(s)


def add_to_workspace_cn(str_name, node_name):
    pass


def add_to_engine_dispatcher(node_name, converter_name, engine):
    pass


def get_class_name(name):
    split = name.split("_")
    return ''.join([x[0].upper()+x[1:] for x in split])


if __name__ == '__main__':
    command = ""
    try:
        command = sys.argv[1]
        name = sys.argv[2]
        engine = sys.argv[4]
    except IndexError:
        pass
    if command == "createnode":
        print("create_node")
        create_node(name, sys.argv[3])
        create_converter(name, engine, sys.argv[3])
    if command == "getclassname":
        print(get_class_name(name))
