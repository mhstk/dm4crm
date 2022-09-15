from typing import List, Optional, cast, Tuple
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

    def check_validity(self) -> bool:
        node: GeneralNode = cast(GeneralNode, self.target_node)
        while True:
            if isinstance(node, InitialNode):
                return True
            node = cast(NonInitialNode, node)
            if not node.get_in_ports():
                return False
            node = node.get_in_port()

    def ident_generator(self):
        self.ident_number += 1
        return self.ident_name + str(self.ident_number)

    def backward_bfs(self):
        temp_q: List = []
        frontier_q: List = [self.target_node, ""]
        delayed = []
        while frontier_q:
            node_ident: Tuple = frontier_q.pop(0)
            node: GeneralNode = node_ident[0]
            nw: NodeWrapper = NodeWrapper(node)
            if node is self.target_node:
                nw.set_out_idents([self.ident_generator() for _ in node.get_out_ports()])
            else:
                if not all([y in [x.get_node() for x in self.wrap_nodes] for y in node.get_out_ports().values()]):
                    delayed.append(node_ident)
                    print(delayed)
                    continue
                frontier_q = delayed + frontier_q
                delayed = []
                for i in range(len(node.get_out_ports().values())):
                    if node_ident[1] and node.get_out_ports()[i]:
                        # nw.set_out_idents(nw.get_out_idents() + [temp_q.pop(0)])
                        nw.set_out_idents(nw.get_out_idents() + [node_ident])
                    else:
                        new_ident: str = self.ident_generator()
                        nw.set_out_idents(nw.get_out_idents() + [new_ident])
            if isinstance(node, NonInitialNode):
                node: NonInitialNode = cast(NonInitialNode, node)
                for nd in node.get_in_ports().values():
                    if nd not in frontier_q and nd not in [x.get_node() for x in self.wrap_nodes]\
                            and nd:
                        new_ident: str = self.ident_generator()
                        nw.set_in_idents(nw.get_in_idents() + [new_ident])
                        frontier_q.append((nd, new_ident))
                print(frontier_q)
                # for i in range(len(node.get_in_ports().values())):
                #     if node.get_in_ports()[i]:
                #         new_ident: str = self.ident_generator()
                #         temp_q.append(new_ident)
                #         nw.set_in_idents(nw.get_in_idents() + [new_ident])

            self.wrap_nodes.append(nw)
            print([x.get_node() for x in self.wrap_nodes])
            input()
            print("******************")
        print([x.get_node() for x in self.wrap_nodes])
        self.wrap_nodes = self.wrap_nodes[::-1]

    def refresh(self) -> None:
        if not self.check_validity():
            raise Exception("Dataflow should always start with an InitialNode")

        self.backward_bfs()
