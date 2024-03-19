import yaml
from nodes import Node, RootNode, Acl
from typing import Dict

def _add_folder_nodes(parent_node: Node, folder: Dict):
    node = Node(folder["name"], parent=parent_node)
    for acl in folder["acls"]:
        node.add_acl(Acl.from_dict(acl))

    if "folders" in folder:
        for subfolder in folder["folders"]:
            _add_folder_nodes(node, subfolder)


def process_acl_config(container_config) -> RootNode:
    """Returns a Tree from JSON configuration"""
    root_node = RootNode(container_config["name"])
    for acl in container_config["acls"]:
        root_node.add_acl(Acl.from_dict(acl))

    for folder in container_config["folders"]:
        _add_folder_nodes(root_node, folder)

    return root_node

def config_from_yaml(filename: str) -> Dict:
    """ Reads a yaml file into a JSON config file"""

    with open(filename, "r") as file:
        acls_config = yaml.safe_load(file)
    
    return acls_config