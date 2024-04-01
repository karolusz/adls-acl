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
import argparse
from .orchestrator import Orchestrator
from .input_parser import config_from_yaml
from .nodes import container_config_to_tree


def main():
    parser = argparse.ArgumentParser(
        description="A small tool for managing data lake gen v2 ACLs"
    )

    parser.add_argument("config_file")
    # parser.add_argument(
    #    "-m",
    #    "--mode",
    #    dest="mode",
    #    choices=["authoritative", "update"],
    #    default="authoritative",
    # )
    args = parser.parse_args()

    acls_config = config_from_yaml(args.config_file)

    for container in acls_config["containers"]:
        tree_root = container_config_to_tree(container)
        Orchestrator(tree_root, acls_config["account"]).process_tree()


if __name__ == "__main__":
    main()
