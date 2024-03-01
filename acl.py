#
# Script to update ACLs.
#
# NOTES:
# UPDATE is not authoritative! It only appends to the ACL.
#
# References:
# https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-acl-python#update-acls-recursively
#
#
# https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control#permissions-inheritance
# default permissions have been set on the parent items before the child items have been created.
# In "authoritative" mode the default acls will have to be pushed down from parent nodes to children.
# How to handle recursive ACLs? Another pass at the end?
#
import logging, yaml, re, argparse
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient,
)
from azure.core.exceptions import ResourceExistsError
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from azure.identity import DefaultAzureCredential, AzureCliCredential
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Self
from dataclasses import dataclass
from orchestrator import orchestrator_factory
from nodes import RootNode, Node, bfs, Acl


class ClientWithACLSupport(ABC):
    @abstractmethod
    def get_access_control():
        pass

    @abstractmethod
    def set_access_control():
        pass

    @abstractmethod
    def update_permissions_recursively():
        pass


class NodeInterface(ABC):
    @abstractmethod
    def create_location():
        pass

    @abstractmethod
    def set_acl():
        pass

    @abstractmethod
    def add_child():
        pass


def get_service_client_token_credential(account_name: str) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.core.windows.net"
    token_credential = DefaultAzureCredential()
    # token_credential = AzureCliCredential()
    service_client = DataLakeServiceClient(account_url, credential=token_credential)

    return service_client


def get_directory_client(
    container: str, parent_dir: str, service_client: DataLakeServiceClient
) -> DataLakeDirectoryClient:

    directory_client = service_client.get_file_system_client(
        file_system=container
    ).get_directory_client(parent_dir)

    return directory_client


def add_folder_nodes(parent_node: Node, folder: Dict):
    node = Node(folder["name"], parent=parent_node)
    for acl in folder["acls"]:
        node.add_acl(Acl.from_dict(acl))

    if "folders" in folder:
        for subfolder in folder["folders"]:
            add_folder_nodes(node, subfolder)


def process_acl_config(container_config):
    root_node = RootNode(container_config["name"])
    for acl in container_config["acls"]:
        root_node.add_acl(Acl.from_dict(acl))

    for folder in container_config["folders"]:
        add_folder_nodes(root_node, folder)

    return root_node


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A small tool for managing data lake gen v2 ACLs"
    )

    parser.add_argument("config_file")
    parser.add_argument(
        "-m",
        "--mode",
        dest="mode",
        choices=["update", "authoritative"],
        default="update",
    )
    args = parser.parse_args()
    account_name = "testingaclscripts"

    # load_dotenv(".env")

    with open(args.config_file, "r") as file:
        acls_config = yaml.safe_load(file)

    sc = get_service_client_token_credential(acls_config["account"])

    if args.mode == "update":
        pass
    if args.mode == "authoritative":
        tree_root = process_acl_config(acls_config["containers"][0])

    orchestrator = orchestrator_factory(args.mode)(tree_root)
    orchestrator.process_tree()
