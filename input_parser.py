import yamale
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
    """Reads a yaml file into dictionary and validates."""

    config_schema = yamale.make_schema("./schema.yml", parser="PyYAML")
    acls_config = yamale.make_data(filename, parser="PyYAML")

    try:
        yamale.validate(config_schema, acls_config)
    except yamale.YamaleError as e:
        for result in e.results:
            print(f"Error validating data {result.data} with {result.schema}\n\t")
            for error in result.errors:
                print("\t%s" % error)
        raise e

    # yamale puts the dict from yaml into a tuple, into a list
    return acls_config[0][0]
