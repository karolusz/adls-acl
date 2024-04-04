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
from .orchestrator import Orchestrator
from .input_parser import config_from_yaml
from .nodes import container_config_to_tree

root_logger = logging.getLogger()  # Root Logger


class EnumCallableMixin:
    """Makes enum callable."""

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)


class HandlerEnum(EnumCallableMixin, Enum):
    stream = logging.StreamHandler
    file = logging.FileHandler


class FormatterStyleEnum(Enum):
    simple = "%(message)s"
    debug = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


@click.group()
@click.option("--debug", default=False, help="Enable debug messages.")
@click.option("--silent", "silent", default=False, help="Suppress logs to stdout.")
@click.option("--log-file", "log_file", default=None, help="Redirect logs to a file.")
def cli(debug, silent, log_file):
    if debug == True:
        root_logger.setLevel(logging.NOTSET)
        format = logging.Formatter(FormatterStyleEnum.debug.value)
    else:
        root_logger.setLevel(logging.INFO)
        format = logging.Formatter(FormatterStyleEnum.simple.value)

    if not silent:
        handler = HandlerEnum.stream(sys.stdout)
        handler.setFormatter(format)
        if not debug:
            handler.addFilter(lambda record: "azure" not in record.name)
        root_logger.addHandler(handler)
    if log_file:
        handler = HandlerEnum.file(filename=log_file, mode="w", encoding="utf-8")
        handler.setFormatter(format)
        if not debug:
            handler.addFilter(lambda record: "azure" not in record.name)
        root_logger.addHandler(handler)


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
