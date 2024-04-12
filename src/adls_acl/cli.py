#
# References:
# https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-acl-python#update-acls-recursively
#
#
# https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control#permissions-inheritance
# default permissions have been set on the parent items before the child items have been created.
#
import click, logging, sys, yaml
from enum import Enum
from .logger import configure_logger
from .orchestrator import Orchestrator
from .input_parser import config_from_yaml
from .nodes import container_config_to_tree, find_node_by_name, Node, Acl, dfs

root_logger = logging.getLogger()  # Root Logger


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug messages.")
@click.option("--silent", is_flag=True, help="Suppress logs to stdout.")
@click.option("--log-file", "log_file", default=None, help="Redirect logs to a file.")
def cli(debug, silent, log_file):
    global root_logger
    root_logger = configure_logger(root_logger, debug, silent, log_file)


@cli.command()
@click.argument(
    "file",
    type=click.File(mode="r", encoding="utf-8", lazy=True),
)
def set_acl(file):
    """Read and set direcotry structure and ACLs from a YAML file."""
    config_str = file.read()
    acls_config = config_from_yaml(config_str)

    for container in acls_config["containers"]:
        tree_root = container_config_to_tree(container)
        Orchestrator(tree_root, acls_config["account"]).process_tree()


@cli.command()
@click.argument("account_name", type=str)
@click.option(
    "--dump-file",
    "outfile",
    type=click.File("w", encoding="utf-8", lazy=True),
    help="A file to which the accounts' data will be writen to.",
)
@click.option(
    "--omit-special",
    "omit",
    is_flag=True,
    help="Omit special ACLs when reading the account.",
)
def get_acl(account_name, outfile, omit):
    """Read the current fs and acls on dirs."""
    # Orchestrator(None, account_name).read_account()
    sc = Orchestrator(None, account_name).sc
    data = {}
    data["account"] = account_name
    data["containers"] = []
    for container in sc.list_file_systems():
        fc = sc.get_file_system_client(container)
        dc = fc._get_root_directory_client()

        root_node = Node(name=fc.file_system_name)

        for acl in dc.get_access_control()["acl"].split(","):
            acl = Acl.from_str(acl)
            if omit and acl.is_special():
                pass
            else:
                root_node.add_acl(acl)

        path_list = fc.get_paths(recursive=True)
        for path in filter(lambda x: x.is_directory == True, path_list):
            dc = fc.get_directory_client(path.name)
            parent_name = ("/").join(path.name.split("/")[:-1])
            parent_node = find_node_by_name(root_node, parent_name)
            node_name = path.name.split("/")[-1]
            node = Node(name=node_name, parent=parent_node)
            for acl in dc.get_access_control()["acl"].split(","):
                acl = Acl.from_str(acl)
                if omit and acl.is_special():
                    pass
                else:
                    node.add_acl(acl)

        # for node in dfs(root=root_node):
        #    print(node)

        data["containers"].append(root_node.to_yaml())
    yaml.dump(data, outfile, sort_keys=False, indent=2)


if __name__ == "__main__":
    cli()
