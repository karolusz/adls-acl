from azure.storage.filedatalake import (
    DataLakeServiceClient,
)
from nodes import RootNode, Node, bfs


class OrchestratorAuthoritative:
    def __init__(self, root: RootNode):
        self.root = root

    def process_tree(self, sc: DataLakeServiceClient):
        for node in bfs(self.root):
            process_node(node)


class OrchestratorUpdate:
    def __init__(self):
        pass


def process_node(node, sc):
    if isinstance(node, RootNode):
        _process_root_node(node, sc)
    else:
        _process_dir_node(node, sc)


def orchestrator_factory(mode):
    if mode == "authoritative":
        return OrchestratorAuthoritative
    elif mode == "update" or mode is None:
        return OrchestratorUpdate
