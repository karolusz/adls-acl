import logging
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
)
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.core.exceptions import ResourceExistsError
from abc import ABC, abstractmethod
from typing import Set

from .nodes import RootNode, Node, bfs, Acl

log = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, root: RootNode, account_name: str):
        self.root = root
        self.sc = _get_service_client_token_credential(account_name)

    def process_tree(self):
        for node in bfs(self.root):
            processor = processor_selector(node)
            dc = processor.get_dir_client(node, self.sc)
            processor.set_acls(node, dc)


class ClientWithACLSupport(ABC):
    @abstractmethod
    def get_access_control(): ...

    @abstractmethod
    def set_access_control(): ...

    # Not needed until update mode is implemented
    # @abstractmethod
    # def update_permissions_recursively(): ...


def _get_service_client_token_credential(account_name: str) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.core.windows.net"
    token_credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url, credential=token_credential)

    return service_client


def _filter_acls_to_preserve(current_acls: Set[Acl]) -> Set[Acl]:
    """Determines which ACLs in the current Node should be preserved in the update"""
    acls_to_preserve = set()

    for acl in current_acls:
        if any([acl.is_mask(), acl.is_other(), acl.is_owner(), acl.is_owner_group()]):
            acls_to_preserve.update((acl,))

    return acls_to_preserve


def _get_current_acls(client: DataLakeDirectoryClient) -> Set[Acl]:
    """Returns a set of ACLs currently set on the directory."""
    current_acls_str = client.get_access_control()["acl"]
    return set([Acl.from_str(acl_str) for acl_str in current_acls_str.split(",")])


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


class Processor(ABC):
    @abstractmethod
    def set_acls(self, node: Node, client: DataLakeDirectoryClient):
        # Get current ACLs to preseve ACLs for
        # Owner, Owner Group, mask, and other (unless specified in input)
        current_acls = _get_current_acls(client)
        acls_to_preserve = _filter_acls_to_preserve(current_acls)
        acls_to_pushdown = set([acl for acl in node.acls if acl.is_default()])

        # Collect ACLs to set
        new_acls = node.acls
        new_acls.update(acls_to_preserve)

        _set_acls(client, new_acls)
        _pushdown_acls(node, acls_to_pushdown)

    @abstractmethod
    def get_dir_client() -> ClientWithACLSupport: ...

    @abstractmethod
    def update_acls(): ...


class ProcessorRoot(Processor):
    def __init__(self):
        pass

    def get_dir_client(self, node: RootNode, client: DataLakeServiceClient):
        """Creates a container if it doesn't exist and returns a file clietn
        to its root directory."""
        log.info("PROCESSING NODE ===========")
        log.info(node)
        if not isinstance(node, RootNode):
            raise TypeError(f"Node of type RootNode was expected")

        try:
            file_client = client.create_file_system(node.name)
        except ResourceExistsError:
            file_client = client.get_file_system_client(file_system=node.name)

        return file_client._get_root_directory_client()

    def set_acls(self, node: Node, client: DataLakeDirectoryClient):
        super().set_acls(node, client)
        client.close()

    def update_acls():
        pass


class ProcessorDir(Processor):
    def __init__(self):
        pass

    def get_dir_client(self, node: Node, client: DataLakeServiceClient):
        """Creates a directory, if it doesn't exist and returns a directory
        client."""
        log.info("PROCESSING NODE ===========")
        log.info(node)

        folder_client = client.get_file_system_client(file_system=node.get_root().name)
        dir_client = folder_client.get_directory_client(node.path)
        dir_client.create_directory()

        return dir_client

    def set_acls(self, node: Node, client: DataLakeDirectoryClient):
        super().set_acls(node, client)

    def update_acls():
        pass


def processor_selector(node):
    if isinstance(node, RootNode):
        return ProcessorRoot()
    elif isinstance(node, Node):
        return ProcessorDir()
