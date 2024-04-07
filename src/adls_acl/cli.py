#
# References:
# https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-acl-python#update-acls-recursively
#
#
# https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control#permissions-inheritance
# default permissions have been set on the parent items before the child items have been created.
#
import click, logging, sys
from enum import Enum
from .logger import configure_logger
from .orchestrator import Orchestrator
from .input_parser import config_from_yaml
from .nodes import container_config_to_tree

root_logger = logging.getLogger()  # Root Logger


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug messages.")
@click.option("--silent", is_flag=True, help="Suppress logs to stdout.")
@click.option("--log-file", "log_file", default=None, help="Redirect logs to a file.")
def cli(debug, silent, log_file):
    global root_logger
    root_logger = configure_logger(root_logger, debug, silent, log_file)


@cli.command()
@click.argument("file", type=click.File(mode="r", encoding="utf-8", lazy=True))
def set_acl(file):
    """Read and set direcotry structure and ACLs from a YAML file."""
    config_str = file.read()
    acls_config = config_from_yaml(config_str)

    for container in acls_config["containers"]:
        tree_root = container_config_to_tree(container)
        Orchestrator(tree_root, acls_config["account"]).process_tree()


if __name__ == "__main__":
    cli()
