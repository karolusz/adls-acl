from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
)
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.core.exceptions import ResourceExistsError
from nodes import RootNode, Node, bfs, Acl
from abc import ABC, abstractmethod


class Orchestrator:
    def __init__(self, account_name: str, root: RootNode):
        self.root = root


class OrchestratorAuthoritative:
    def __init__(self, account_name: str, root: RootNode):
        self.root = root
        self.sc = self._get_service_client_token_credential(account_name)

    def _get_service_client_token_credential(
        self,
        account_name: str,
    ) -> DataLakeServiceClient:
        account_url = f"https://{account_name}.dfs.core.windows.net"
        token_credential = DefaultAzureCredential()
        service_client = DataLakeServiceClient(account_url, credential=token_credential)

        return service_client

    def process_tree(self, sc: DataLakeServiceClient):
        pass


class OrchestratorUpdate:
    def __init__(self, root: RootNode):
        self.root = root

    def process_tree(self, sc: DataLakeServiceClient):
        pass


class ClientWithACLSupport(ABC):
    @abstractmethod
    def get_access_control(): ...

    @abstractmethod
    def set_access_control(): ...

    @abstractmethod
    def update_permissions_recursively(): ...


class Processor(ABC):
    @abstractmethod
    def set_acls(self, node: Node, client: DataLakeDirectoryClient):
        # Get current ACLs to presercve ACLs for
        # Owner, Owner Group, mask, and other (unless specified in input)
        current_acls = client.get_access_control()["acl"]
        acls_to_preserve = set()

        for acl_str in current_acls.split(","):
            acl = Acl.from_str(acl_str)
            if any(
                [acl.is_mask(), acl.is_other(), acl.is_owner(), acl.is_owner_group()]
            ):
                acls_to_preserve.update((acl,))

        # Catch all default acls from input acls
        acls_to_pushdown = set([acl for acl in node.acls if acl.is_default()])

        # Update ACLs
        new_acls = node.acls
        new_acls.update(acls_to_preserve)

        print("new acls")
        for acl in new_acls:
            print(acl)
            client.set_access_control(acl=acl)

        # Push down acls to child nodes
        for child_node in node.children:
            child_node.acls.update(acls_to_pushdown)

    @abstractmethod
    def get_dir_client() -> ClientWithACLSupport: ...

    @abstractmethod
    def update_acls(): ...


class ProcessorRoot(Processor):
    def __init__(self):
        pass

    def get_dir_client(self, node: RootNode, client: DataLakeServiceClient):
        print("PROCESSING NODE ===========")
        print(node)
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
        print("PROCESSING NODE ===========")
        print(node)

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


def orchestrator_factory(mode):
    if mode == "authoritative":
        return OrchestratorAuthoritative
    elif mode == "update" or mode is None:
        return OrchestratorUpdate
