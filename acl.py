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
import logging, re, argparse
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
from orchestrator import Orchestrator
from nodes import RootNode, Node, bfs, Acl
from input_parser import process_acl_config, config_from_yaml


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

    # load_dotenv(".env")
    
    acls_config = config_from_yaml(args.config_file)

    if args.mode == "update":
        print("NOT IMPLEMENTED")
        raise ValueError

    for container in acls_config["containers"]:
        tree_root = process_acl_config(container)
        Orchestrator(tree_root, acls_config["account"]).process_tree()

