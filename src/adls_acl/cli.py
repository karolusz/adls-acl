#
# References:
# https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-acl-python#update-acls-recursively
#
#
# https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control#permissions-inheritance
# default permissions have been set on the parent items before the child items have been created.
#
import logging

import click
import yaml

from .input_parser import config_from_yaml
from .logger import configure_logger
from .nodes import container_config_to_tree
from .orchestrator import Orchestrator
from .auth import AUTH_SUPPORTED_OPTIONS

root_logger = logging.getLogger()  # Root Logger


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug messages.")
@click.option("--silent", is_flag=True, help="Suppress logs to stdout.")
@click.option("--log-file", "log_file", default=None, help="Redirect logs to a file.")
def cli(debug, silent, log_file, root_logger=root_logger):
    root_logger = configure_logger(root_logger, debug, silent, log_file)


@cli.command()
@click.argument(
    "file",
    type=click.File(mode="r", encoding="utf-8", lazy=True),
)
@click.option(
    "--auth-method",
    type=click.Choice(AUTH_SUPPORTED_OPTIONS, case_sensitive=False),
    default="default",
    help="Azure AD Authentication method",
)
@click.option("--auth-opt", type=click.Tuple([str, str]), multiple=True)
def set_acl(file, auth_method, auth_opt):
    """Read and set direcotry structure and ACLs from a YAML file."""
    auth_opt = {x[0]: x[1] for x in auth_opt}
    config_str = file.read()
    acls_config = config_from_yaml(config_str)
    o = Orchestrator(
        acls_config["account"], auth_method=auth_method, auth_kwargs=auth_opt
    )

    for container in acls_config["containers"]:
        tree_root = container_config_to_tree(container)
        o.process_tree(tree_root)


@cli.command()
@click.argument("account_name", type=str)
@click.argument(
    "outfile",
    type=click.File("w", encoding="utf-8", lazy=True),
)
@click.option(
    "--omit-special",
    "omit_special",
    is_flag=True,
    help="Omit special ACLs when reading the account.",
)
@click.option(
    "--auth-method",
    type=click.Choice(AUTH_SUPPORTED_OPTIONS, case_sensitive=False),
    default="default",
    help="Azure AD Authentication method",
)
@click.option("--auth-opt", type=click.Tuple([str, str]), multiple=True)
def get_acl(account_name, outfile, omit_special, auth_method, auth_opt):
    """Read the current fs and acls on dirs."""
    data = Orchestrator(
        account_name, auth_method=auth_method, auth_kwargs=auth_opt
    ).read_account(omit_special=omit_special)
    yaml.dump(data, outfile, sort_keys=False, indent=2)


if __name__ == "__main__":
    cli()
