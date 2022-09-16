from typing import List, Optional, cast, Dict
from .node.general_node import GeneralNode
from .node.initial_node import InitialNode
from .node.non_initial_node import NonInitialNode
from .node_wrapper import NodeWrapper


class Dataflow:
    def __init__(self) -> None:
        self.nodes: List[GeneralNode] = []
        self.wrap_nodes: List[NodeWrapper] = []
        self.target_node: Optional[GeneralNode] = None
        self.ident_number = -1
        self.ident_name = "ident"
        self.node_connections: List = []

    def set_node_connections(self, connections: List):
        self.node_connections = connections

    def get_from_port_by_connection(self, from_node: GeneralNode, dest_node: GeneralNode, dest_port: int):
        # print("node_connections: ", self.node_connections)
        for f_node, d_node, f_port, d_port in self.node_connections:
            if f_node == from_node and d_node == dest_node and dest_port == d_port:
                return f_port
        return -1

    def check_validity(self) -> bool:
        node: GeneralNode = cast(GeneralNode, self.target_node)
        while True:
            if isinstance(node, InitialNode):
                return True
            print(node)
            node: NonInitialNode = cast(NonInitialNode, node)
            if not node.get_in_port(0):
                return False
            node = node.get_in_port()

    def ident_generator(self):
        self.ident_number += 1
        return self.ident_name + str(self.ident_number)

    @staticmethod
    def dependency_nodes(node: GeneralNode):
        deps: List = []
        frontier: List = [node]
        while True:
            node = frontier.pop(0)
            if isinstance(node, InitialNode):
                return deps
            node = cast(NonInitialNode, node)
            print(node)
            for nd in node.get_in_ports().values():
                if nd not in deps:
                    deps.append(nd)
                frontier.append(nd)

    def backward_bfs(self):
        deps: List = Dataflow.dependency_nodes(self.target_node)
        frontier_q: List = [self.target_node]
        connections: Dict = {}
        delayed = []
        while frontier_q:
            node: GeneralNode = frontier_q.pop(0)
            nw: NodeWrapper = NodeWrapper(node)
            if node is self.target_node:
                for port, nd in node.get_out_ports().items():
                    nw.set_out_ident(port, self.ident_generator())
            else:
                if not all([y in [x.get_node() for x in self.wrap_nodes] or y is None or y not in deps
                            for y in node.get_out_ports().values()]):
                    delayed.append(node)
                    # print("delayed:", delayed)
                    continue
                frontier_q = delayed + frontier_q
                delayed = []

                # connections = dict(sorted(connections.items(), key=lambda item: item[0][1]))
                for port, nd in node.get_out_ports().items():
                    # print(node, nd)
                    if not nd or nd not in deps:
                        nw.set_out_ident(port, self.ident_generator())

                for connection in list(connections):
                    if connection[0] == node:
                        nw.set_out_ident(connection[1], connections[connection])
                        del connections[connection]
                # print(node.get_out_ports())
            # print("Exploring:", node)
            # print("final_out_idents", nw.get_out_idents())

            if isinstance(node, NonInitialNode):
                node: NonInitialNode = cast(NonInitialNode, node)
                nd: GeneralNode
                for in_port, nd in node.get_in_ports().items():

                    new_ident = self.ident_generator()
                    nw.set_in_ident(in_port, new_ident)
                    if nd:
                        port = self.get_from_port_by_connection(nd, node, in_port)
                        if port != -1:
                            connections[(nd, port)] = new_ident
                            if nd not in frontier_q and nd not in delayed:
                                frontier_q.append(nd)
                # print("final_in_indents: ", nw.get_in_idents())
                # print("connections: ", connections)
                # print("frontier:", frontier_q)

            self.wrap_nodes.append(nw)
            # print("wrap_nodes:", [x.get_node() for x in self.wrap_nodes])
            # input()
            # print("******************")
        # print([x.get_node() for x in self.wrap_nodes])
        self.wrap_nodes = self.wrap_nodes[::-1]

    def refresh(self) -> None:
        if not self.check_validity():
            raise Exception("Dataflow should always start with an InitialNode")

        self.backward_bfs()
