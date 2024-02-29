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
from typing import List, Dict, Optional
from dataclasses import dataclass


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


@dataclass
class Acl:
    p_type: str
    oid: str
    permissions: str
    scope: Optional[str] = None

    @classmethod
    def from_str(cls, acl_str: str) :
        """Returns an instance of Acl from string from get_access_control call"""
        acl_str = acl_str.split(":")
        if len(acl_str) == 3:
            p_type, oid, permissions = acl_str
            return Acl(p_type, oid, permissions)
        elif len(acl_str) == 4:
            scope, p_type, oid, permissions = acl_str
            return Acl(p_type, oid, permissions, scope=scope)

    @classmethod
    def from_dict(cls, acl_dict: Dict) :
        """Returns an instance of Acl from a dict from yaml input"""
        if "acl" in acl_dict:
            acl = acl_dict["acl"]
        else:
            acl = acl_dict["default_acl"]
        if "default" in acl_dict:
            scope = acl_dict["scope"]
        else:
            scope = None

        return Acl(acl_dict["type"], acl_dict["oid"], acl, scope=scope)

    def __str__(self):
        if self.scope is None:
            s = f"{self.p_type}:{self.oid}:{self.permissions}"
        else:
            s = f"{self.scope}:{self.p_type}:{self.oid}:{self.permissions}"
        return s

    def is_owner(self):
        """Checks if it is object owner ACL"""
        return True if (self.oid == "" and self.p_type == "user") else False

    def is_owner_group(self):
        """Check if it is owner group ACL"""
        return True if (self.oid == "" and self.p_type == "group") else False

    def is_mask(self):
        """Check if mask"""
        return True if (self.p_type == "mask") else False


class Node:
    def __init__(self, name: str, parent=None):
        self.name = name
        self.children = []
        self.parent = parent
        self.acls = []

    def __str__(self):
        """Print node nicely"""
        s = f"Path to node: {self.path}"
        s += f"\n Acls from input:"
        for acl in self.acls:
            s += f"\n{str(acl)}"

        return s

    @property
    def path(self):
        """Get the path from root of the container to the Node"""
        if self.parent is not None:
            return f"{self.parent.path}/{self.name}"
        else:
            return f"{self.name}"

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, value: Self):
        """Sets a parent node. Also registers the child @parent"""
        if value is not None:
            if not isinstance(value, Node):
                raise TypeError(f"Parent must be of type, {type(self)}")
            self._parent = value
            self._parent.add_child(self)
        else:
            self._parent = None

    def add_acl(self, acl: Acl):
        """Append Acl to the list of Acls"""
        if not isinstance(acl, Acl):
            raise TypeError(f"Acl must be of type, {type(Acl)}")
        self.acls.append(acl)

    def add_child(self, child: Acl):
        self.children.append(child)

    def _create_location(self, client):
        pass

    def _set_acls(self, client):
        pass

    def process(self, client: FileSystemClient, pushed_down_acl=[]):
        """Processes the node"""
        for acl in pushed_down_acl:
            self.add_acl(acl)

        self._create_location(client)
        default_acls = self._set_acls(client)

        for child_node in self.children:
            child_node.process(default_acls)


class RootNode(Node):
    def __init__(self, name: str, parent=None):
        super().__init__(name, parent)

    def process(self, client: DataLakeServiceClient):
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


def update_permission_recursively(
    client: ClientWithACLSupport,
    oid: str,
    p_type: str,
    acl: str,
    is_default_scope: bool = False,
) -> None:
    permissions = f"{p_type}:{oid}:{acl.upper()}"
    if is_default_scope:
        permissions = f"default:{permissions}"
    print(permissions)
    client.update_access_control_recursive(acl=permissions)

    return None


def update_permission_nonrecursively(
    client: ClientWithACLSupport,
    oid: str,
    p_type: str,
    acl: str,
    is_default_scope: bool = False,
) -> None:
    current_acls = client.get_access_control()["acl"]
    # Check if acl for user:type exists
    # DEV
    # for str_acl in current_acls.split(","):
    #    acl = Acl.from_str(str_acl)
    #    print(acl)
    #    input()
    acl_prefix = f"{p_type}:{oid}:"
    if is_default_scope:
        acl_prefix = f"default:{acl_prefix}"
    # pattern = rf"(?<!default:){p_type}:{re.escape(oid)}:"
    pattern = rf"(?<!default:){re.escape(acl_prefix)}"
    if re.search(pattern, current_acls) is not None:
        pattern = rf"({pattern})(.*?)($|,|\n)"
        new_acls = re.sub(pattern, rf"\1{acl.lower()}\3", current_acls)
    else:
        new_acls = f"{current_acls},{acl_prefix}{acl.lower()}"
    print(new_acls)
    client.set_access_control(acl=new_acls)

    return None


def update_permissions(client: ClientWithACLSupport, acl: Dict):
    if "recursive" in acl:
        if acl["recursive"] == True:
            if "acl" in acl:
                update_permission_recursively(
                    client=client, oid=acl["oid"], p_type=acl["type"], acl=acl["acl"]
                )
            if "default_acl" in acl:
                update_permission_recursively(
                    client=client,
                    oid=acl["oid"],
                    p_type=acl["type"],
                    acl=acl["default_acl"],
                    is_default_scope=True,
                )
            return None
    else:
        if "acl" in acl:
            update_permission_nonrecursively(
                client=client, oid=acl["oid"], p_type=acl["type"], acl=acl["acl"]
            )
        if "default_acl" in acl:
            print("DEFAULT ACL:,", acl)
            update_permission_nonrecursively(
                client=client,
                oid=acl["oid"],
                p_type=acl["type"],
                acl=acl["default_acl"],
                is_default_scope=True,
            )


def update_acls(client: ClientWithACLSupport, acls: List[object]):
    for acl in acls:
        update_permissions(client, acl)


def traverse_folders(fc: FileSystemClient, path, folder_config):
    if path == "":
        path = folder_config["name"]
    else:
        path = f"{path}/{folder_config['name']}"
    print(f"PATH: {path}")
    dc = fc.get_directory_client(path)
    if not dc.exists():
        dc.create_directory()

    update_acls(dc, folder_config["acls"])
    dc.close()

    if "folders" in folder_config:
        for folder in folder_config["folders"]:
            traverse_folders(fc, path, folder)

    return None


def traverse_containers(sc: DataLakeServiceClient, containers: List[object]):
    for container in containers:
        container_name = container["name"]
        # Create container if it doesn't exist
        # Get the file system client
        try:
            fc = sc.create_file_system(container_name)
        except ResourceExistsError:
            fc = sc.get_file_system_client(file_system=container_name)

        # Update acls on the root directory
        update_acls(fc._get_root_directory_client(), container["acls"])
        for folder in container["folders"]:
            traverse_folders(fc, "", folder)
        fc.close()


def add_folder_nodes(parent_node: Node, folder: Dict):
    node = Node(folder["name"], parent=parent_node)
    for acl in folder["acls"]:
        node.add_acl(Acl.from_dict(acl))

    if "folders" in folder:
        for subfolder in folder["folders"]:
            print(subfolder)
            add_folder_nodes(node, subfolder)
    print(node)


def process_acl_config(container_config):
    root_node = RootNode(container_config["name"])
    for acl in container_config["acls"]:
        root_node.add_acl(Acl.from_dict(acl))

    for folder in container_config["folders"]:
        print(folder)
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

    # account_tree = config_file_to_tree(acls_config["account"])
    # print(acls_config["account"])
    sc = get_service_client_token_credential(acls_config["account"])

    if args.mode == "update":
        traverse_containers(sc, acls_config["containers"])
    if args.mode == "authoritative":
        tree_root = process_acl_config(acls_config["containers"][0])
