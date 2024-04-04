from typing import Self, Optional, Dict
from dataclasses import dataclass


@dataclass
class Acl:
    p_type: str
    oid: str
    permissions: str
    scope: Optional[str] = None

    @classmethod
    def from_str(cls, acl_str: str):
        """Returns an instance of Acl from string from get_access_control call"""
        acl_str = acl_str.split(":")
        if len(acl_str) == 3:
            p_type, oid, permissions = acl_str
            return Acl(p_type, oid, permissions)
        elif len(acl_str) == 4:
            scope, p_type, oid, permissions = acl_str
            return Acl(p_type, oid, permissions, scope=scope)

    @classmethod
    def from_dict(cls, acl_dict: Dict):
        """Returns an instance of Acl from a dict from yaml input"""
        if "acl" in acl_dict:
            acl = acl_dict["acl"]
        if "scope" in acl_dict:
            scope = acl_dict["scope"]
        else:
            scope = None

        acl = Acl(acl_dict["type"], acl_dict["oid"], acl, scope=scope)
        return acl

    def __str__(self):
        if self.scope is None:
            s = f"{self.p_type}:{self.oid}:{self.permissions}"
        else:
            s = f"{self.scope}:{self.p_type}:{self.oid}:{self.permissions}"
        return s

    def __eq__(self, other):
        return (
            self.p_type == other.p_type
            and self.oid == other.oid
            and self.scope == other.scope
        )

    def __hash__(self):
        return hash((self.p_type, self.oid, self.scope))

    def is_owner(self):
        """Checks if it is object owner ACL"""
        return True if (self.oid == "" and self.p_type == "user") else False

    def is_owner_group(self):
        """Check if it is owner group ACL"""
        return True if (self.oid == "" and self.p_type == "group") else False

    def is_mask(self):
        """Check if mask"""
        return True if (self.p_type == "mask") else False

    def is_other(self):
        """Check if it is ACL for other"""
        return True if (self.p_type == "other") else False

    def is_default(self):
        """Check if ACL is default"""
        return True if (self.scope == "default") else False


class Node:
    def __init__(self, name: str, parent=None):
        self.name = name
        self.children = []
        self.parent = parent
        self.acls = set()

    def __str__(self):
        """Print node nicely"""
        s = f"Path to node: {self.path}"
        s += f"\nAcls from input:"
        for acl in self.acls:
            s += f"\n\t{str(acl)}"

        return s

    @property
    def path(self):
        """Get the path from root of the container to the Node"""
        if self.parent is not None and not isinstance(self.parent, RootNode):
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

    def get_root(self):
        root = self
        while root.parent is not None:
            root = root.parent
        return root

    def add_acl(self, acl: Acl):
        """Append Acl to the list of Acls"""
        if not isinstance(acl, Acl):
            raise TypeError(f"Acl must be of type, {type(Acl)}")
        self.acls.update((acl,))

    def add_child(self, child: Acl):
        self.children.append(child)


class RootNode(Node):
    def __init__(self, name: str, parent=None):
        super().__init__(name, parent)


def _add_folder_nodes(parent_node: Node, folder: Dict):
    node = Node(folder["name"], parent=parent_node)
    for acl in folder["acls"]:
        node.add_acl(Acl.from_dict(acl))

    if "folders" in folder:
        for subfolder in folder["folders"]:
            _add_folder_nodes(node, subfolder)


def container_config_to_tree(container_config) -> RootNode:
    """Returns a Tree from JSON configuration"""
    root_node = RootNode(container_config["name"])
    for acl in container_config["acls"]:
        root_node.add_acl(Acl.from_dict(acl))

    for folder in container_config["folders"]:
        _add_folder_nodes(root_node, folder)

    return root_node


def bfs(root: RootNode):
    """Breadth-first traversal of the tree"""
    queue = [root]

    while queue:
        node = queue.pop()
        for child_node in node.children:
            queue.append(child_node)
        yield node
