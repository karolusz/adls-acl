import logging
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
)
from azure.core.exceptions import ResourceExistsError
from abc import ABC, abstractmethod
from typing import Set, Dict, Any

from .nodes import Node, bfs, Acl, find_node_by_name
from .auth import get_service_client

log = logging.getLogger(__name__)


class Orchestrator:
    def __init__(
        self, account_name: str, auth_method: str = "default", **auth_kwargs: Any
    ):
        self.sc = get_service_client(account_name, auth_method, **auth_kwargs)
        self.account_name = account_name

    def process_tree(self, root):
        # First pass to set non-recursive ACLs and materialzie new nodes
        # in the account
        for node in bfs(root):
            log.info("PROCESSING NODE ===========")
            log.info(node)
            processor = processor_selector(node)
            dc = processor.get_dir_client(node, self.sc)
            processor.set_acls(node, dc)

        # Second pass to set recursive ACLs
        for node in bfs(root):
            processor = processor_selector(node)
            if any([acl.is_recursive() for acl in node.acls]):
                log.info("Applying recursive ACLs")
                log.info(f"Path to node: {node.path}")
                dc = processor.get_dir_client(node, self.sc)
                processor.update_acls_recursive(node, dc)

    def read_account(self, omit_special: bool = False) -> Dict:
        data = {}
        data["account"] = self.account_name
        data["containers"] = []
        for container in self.sc.list_file_systems():
            # Create a root node
            fc = self.sc.get_file_system_client(container)
            dc = fc._get_root_directory_client()
            root_node = Node(name=fc.file_system_name)
            for acl in _get_current_acls(dc, omit_special):
                root_node.add_acl(acl)

            # Add nodes to the tree
            path_list = fc.get_paths(recursive=True)
            for path in filter(lambda x: x.is_directory == True, path_list):
                dc = fc.get_directory_client(path.name)
                parent_name = ("/").join(path.name.split("/")[:-1])
                parent_node = find_node_by_name(root_node, parent_name)
                node_name = path.name.split("/")[-1]
                node = Node(name=node_name, parent=parent_node)
                for acl in _get_current_acls(dc, omit_special):
                    node.add_acl(acl)

            data["containers"].append(root_node.to_yaml())

            return data


class ClientWithACLSupport(ABC):
    @staticmethod
    @abstractmethod
    def get_access_control(): ...

    @staticmethod
    @abstractmethod
    def set_access_control(): ...

    @staticmethod
    @abstractmethod
    def update_access_control_recursive(): ...


def _filter_acls_to_preserve(current_acls: Set[Acl]) -> Set[Acl]:
    """Determines which ACLs in the current Node should be preserved in the update"""
    acls_to_preserve = set()

    for acl in current_acls:
        if any([acl.is_mask(), acl.is_other(), acl.is_owner(), acl.is_owner_group()]):
            acls_to_preserve.update((acl,))

    return acls_to_preserve


def _get_current_acls(
    client: DataLakeDirectoryClient, omit_special: bool = False
) -> Set[Acl]:
    """Returns a set of ACLs currently set on the directory."""
    current_acls_str = client.get_access_control()["acl"]
    acls = set([Acl.from_str(acl_str) for acl_str in current_acls_str.split(",")])
    if omit_special:
        acls = set([acl for acl in acls if not acl.is_special()])
    return acls


def _set_acls(client: DataLakeDirectoryClient, acls: Set[Acl]) -> None:
    """Set ACLs from the set on the node"""
    log.info("Setting new acls:")
    for acl in acls:
        log.info(f"\t{str(acl)}")
        client.set_access_control(acl=acl)


def _pushdown_acls(node: Node, acls: Set[Acl]) -> None:
    """Pushdown selected ACLs to the children of the node"""
    for child_node in node.children:
        child_node.acls.update(acls)


def _update_access_control_recursive(
    client: DataLakeDirectoryClient, acls: Set[Acl], retries: int = 3
) -> None:
    """Update ACLs. The easiest way to apply ACLs recusively.
    https://learn.microsoft.com/en-us/python/api/azure-storage-file-datalake/azure.storage.filedatalake.datalakedirectoryclient?view=azure-python#azure-storage-filedatalake-datalakedirectoryclient-update-access-control-recursive
    """
    for acl in acls:
        continuation_token = None
        for i in range(0, retries):
            change_result = client.update_access_control_recursive(
                acl=acl, continue_on_failure=True, continuation_token=continuation_token
            )
            continuation_token = change_result.get("continue")
            if continuation_token is None:
                break


class Processor(ABC):
    @staticmethod
    @abstractmethod
    def set_acls(node: Node, client: DataLakeDirectoryClient):
        # Get current ACLs to preseve ACLs for
        # Owner, Owner Group, mask, and other
        # they will only change if specifie in input
        current_acls = _get_current_acls(client)
        acls_to_preserve = _filter_acls_to_preserve(current_acls)
        acls_to_pushdown = set([acl for acl in node.acls if acl.is_default()])

        # Collect ACLs to set
        new_acls = node.acls
        new_acls.update(acls_to_preserve)

        _set_acls(client, new_acls)
        _pushdown_acls(node, acls_to_pushdown)

    @staticmethod
    @abstractmethod
    def get_dir_client() -> ClientWithACLSupport: ...

    @staticmethod
    @abstractmethod
    def update_acls_recursive(node: Node, client: DataLakeDirectoryClient) -> None:
        recursive_acls = set([acl for acl in node.acls if acl.is_recursive()])
        log.info("Recursive Acls:")
        for acl in recursive_acls:
            log.info(f"\t {acl}")
        _update_access_control_recursive(client, recursive_acls, retries=3)


class ProcessorRoot(Processor):

    @staticmethod
    def get_dir_client(node: Node, client: DataLakeServiceClient):
        """Creates a container if it doesn't exist and returns a file clietn
        to its root directory."""
        if node.is_root == False:
            raise ValueError("Node is not the root!")

        try:
            fs_client = client.create_file_system(node.name)
        except ResourceExistsError:
            fs_client = client.get_file_system_client(file_system=node.name)

        return fs_client._get_root_directory_client()

    @staticmethod
    def set_acls(node: Node, client: DataLakeDirectoryClient):
        super(ProcessorRoot, ProcessorRoot).set_acls(node, client)
        client.close()

    @staticmethod
    def update_acls_recursive(node: Node, client: DataLakeDirectoryClient):
        super(ProcessorRoot, ProcessorRoot).update_acls_recursive(node, client)
        client.close()


class ProcessorDir(Processor):

    @staticmethod
    def get_dir_client(node: Node, client: DataLakeServiceClient):
        """Creates a directory, if it doesn't exist and returns a directory
        client."""
        folder_client = client.get_file_system_client(file_system=node.get_root().name)
        dir_client = folder_client.get_directory_client(node.path_in_file_system)
        dir_client.create_directory()

        return dir_client

    @staticmethod
    def set_acls(node: Node, client: DataLakeDirectoryClient):
        super(ProcessorDir, ProcessorDir).set_acls(node, client)

    @staticmethod
    def update_acls_recursive(node: Node, client: DataLakeDirectoryClient):
        super(ProcessorDir, ProcessorDir).update_acls_recursive(node, client)
        client.close()


def processor_selector(node):
    if node.is_root:
        return ProcessorRoot()
    else:
        return ProcessorDir()
