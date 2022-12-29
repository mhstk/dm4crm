from ...baseengine.converter import Converter
from ....node.datamining.mini_batch_k_means_cluster_node import MiniBatchKMeansClusterNode


class MiniBatchKMeansClusterConverter(Converter):

    def __init__(self):
        super().__init__()

    def convert(self) -> str:
        node: MiniBatchKMeansClusterNode = self.get_node_wrapper().get_node()
        out_port_ident = self.get_node_wrapper().get_out_idents()[0]
        in_port_ident = self.get_node_wrapper().get_in_idents()[0]
        in_port_ident1 = self.get_node_wrapper().get_in_idents()[1]
        code_str = ""

        code_str += 'from sklearn.cluster import MiniBatchKMeans\n'
        code_str += f'{out_port_ident} = MiniBatchKMeans(  '
        if node.n_clusters:
            code_str += f'n_clusters={node.n_clusters} ,'
        if node.init:
            code_str += f'init="{node.init}" ,'
        if node.max_iter:
            code_str += f'max_iter={node.max_iter} ,'
        if node.batch_size:
            code_str += f'batch_size={node.batch_size} ,'

        code_str = code_str[:-1]
        code_str += f').fit({in_port_ident}, {in_port_ident1}.values.ravel())'

        return code_str

