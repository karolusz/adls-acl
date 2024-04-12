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

    def to_yaml(self):
        """Returns a dict reprentaion"""
        data = {
            "oid": self.oid,
            "type": self.p_type,
            "acl": self.permissions,
        }
        if self.scope is not None:
            data["scope"] = self.scope

        return data


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
    def path_in_file_system(self):
        """Get the path from root of the container to the Node"""
        if self.parent is not None and self.parent.parent is not None:
            return f"{self.parent.path_in_file_system}/{self.name}"
        else:
            return self.name

    @property
    def path(self):
        """Get the path from root (including the root name)"""
        if self.parent is not None:
            return f"{self.parent.path}/{self.name}"
        else:
            return f"{self.name}"

    @property
    def is_root(self):
        return True if self.parent == None else False

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

    def to_yaml(self):
        """Returns a dict reprentaion"""
        data = {"name": self.name, "acls": [acl.to_yaml() for acl in self.acls]}
        if len(self.children) > 0:
            data["folders"] = [child.to_yaml() for child in self.children]

        return data


def _add_folder_nodes(parent_node: Node, folder: Dict):
    node = Node(folder["name"], parent=parent_node)
    for acl in folder["acls"]:
        node.add_acl(Acl.from_dict(acl))

    if "folders" in folder:
        for subfolder in folder["folders"]:
            _ = _add_folder_nodes(node, subfolder)

    return node  # The root node will be returend to the original caller


def find_node_by_name(root_node, full_name: str) -> Node:
    """Return the node by its name (path in the filesystem: from container node)"""
    """ Should i just have a Tree class instead with index by name for nodes?"""
    # Names are path from contianer root: dir1/dir2/dir3

    path_arr = full_name.split("/")
    # if the name of the node is only 1-level and the search is on the root level
    # return root node immediatley
    if path_arr[0] == "" and root_node.is_root:
        return root_node

    current_dir = path_arr[0]
    path_reminder = ("/").join(path_arr[1:])
    node = None

    for child_node in root_node.children:
        if child_node.name == current_dir:
            node = child_node
            break

    if len(path_reminder) > 0 and node is not None:
        node = find_node_by_name(node, full_name=path_reminder)

    return node


def container_config_to_tree(container_config) -> Node:
    """Returns a Tree from JSON configuration"""
    return _add_folder_nodes(None, container_config)


def bfs(root: Node):
    """Breadth-first traversal of the tree"""
    queue = [root]

    while queue:
        node = queue.pop(0)
        for child_node in node.children:
            queue.append(child_node)
        yield node


def dfs(root: Node):
    """Depth-frist traversal of the tree"""
    stack = [root]

    while stack:
        node = stack.pop()
        for child_node in node.children:
            stack.append(child_node)
        yield node
