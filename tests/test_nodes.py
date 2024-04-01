import pytest, copy
from adls_acl import nodes


@pytest.fixture(scope="module")
def container_dict():
    return {
        "name": "test_container",
        "acls": [
            {
                "oid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                "type": "user",
                "acl": "r-x",
            },
        ],
        "folders": [
            {
                "name": "test_folder",
                "acls": [
                    {
                        "oid": "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy",
                        "type": "user",
                        "acl": "--x",
                    },
                ],
                "folders": [
                    {
                        "name": "test_subfolder_one",
                        "acls": [
                            {
                                "oid": "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy",
                                "type": "user",
                                "acl": "--x",
                            }
                        ],
                    }
                ],
            }
        ],
    }


@pytest.fixture(scope="module")
def dummy_acl():
    return nodes.Acl(p_type="user", oid="zzzzzzz", permissions="---")


def _dict_to_tree(container_config):
    return nodes.container_config_to_tree(container_config)


def _counter(iter):
    """Counts the elements in a generator"""
    return sum(1 for _ in iter)


def test_container_config_to_tree(container_dict):
    root = _dict_to_tree(container_dict)

    assert isinstance(root, nodes.RootNode)
    assert len(root.children) == 1
    assert root.name == "test_container"
    assert all([isinstance(x, nodes.Acl) for x in root.children[0].acls])


def test_bfs(container_dict):
    root = _dict_to_tree(container_dict)
    count = _counter(nodes.bfs(root))
    assert count == 3


class TestNode:
    @pytest.fixture(scope="class")
    def rootnode(self, container_dict):
        """Get the rootnode of the test tree"""
        return _dict_to_tree(container_dict)

    @pytest.fixture(scope="class")
    def leafnode(self, rootnode):
        """Get a any leaf node of the test tree"""
        node = rootnode
        while len(node.children) > 0:
            node = node.children[0]

        return node

    def test_path_on_root_node(self, rootnode):
        assert rootnode.path == "test_container"

    def test_path_on_leaf_node(self, leafnode):
        print(leafnode.path)
        assert leafnode.path == "test_folder/test_subfolder_one"

    def test_get_root(self, rootnode, leafnode):
        assert leafnode.get_root() == rootnode

    def test_add_child(self, leafnode):
        parent = copy.deepcopy(leafnode)
        child = copy.deepcopy(leafnode)

        parent.add_child(child)

        assert parent.children[0] == child
        assert len(child.children) == 0

    def test_set_parent(self, leafnode):
        parent = copy.deepcopy(leafnode)
        child = copy.deepcopy(leafnode)

        child.parent = parent

        assert child.parent == parent

    def test_add_acl(self, leafnode, dummy_acl):
        leafnode.add_acl(dummy_acl)

        assert len(leafnode.acls) == 2


class TestAcl:
    @pytest.fixture(scope="class")
    def acl_str(self):
        return "group:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:rwx"

    @pytest.fixture(scope="class")
    def acl_str_default(self):
        return "default:group:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:rwx"

    @pytest.fixture(scope="class")
    def acl_str_owner(self):
        return "user::rwx"

    @pytest.fixture(scope="class")
    def acl_str_owner_group(self):
        return "group::rwx"

    @pytest.fixture(scope="class")
    def acl_str_mask(self):
        return "mask::rwx"

    @pytest.fixture(scope="class")
    def acl_str_other(self):
        return "other::rwx"

    @pytest.fixture(scope="class")
    def acl_dict(self):
        return {
            "type": "group",
            "oid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            "acl": "rwx",
        }

    @pytest.fixture(scope="class")
    def acl_dict_default(self):
        return {
            "scope": "default",
            "type": "group",
            "oid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            "acl": "rwx",
        }

    def test_from_str(self, acl_str):
        acl = nodes.Acl.from_str(acl_str)

        assert isinstance(acl, nodes.Acl)
        assert acl.scope == None
        assert acl.permissions == "rwx"
        assert acl.p_type == "group"

    def test_from_str_scope(self, acl_str_default):
        acl = nodes.Acl.from_str(acl_str_default)

        assert isinstance(acl, nodes.Acl)
        assert acl.scope == "default"
        assert acl.permissions == "rwx"
        assert acl.p_type == "group"

    def test_from_dict(self, acl_dict):
        acl = nodes.Acl.from_dict(acl_dict)

        assert isinstance(acl, nodes.Acl)
        assert acl.scope == None
        assert acl.permissions == "rwx"
        assert acl.p_type == "group"

    def test_from_dict_scope(self, acl_dict_default):
        acl = nodes.Acl.from_dict(acl_dict_default)

        assert isinstance(acl, nodes.Acl)
        assert acl.scope == "default"
        assert acl.permissions == "rwx"
        assert acl.p_type == "group"

    def test_is_defualt(self, acl_str_default):
        acl = nodes.Acl.from_str(acl_str_default)

        assert acl.is_default()

    def test_is_owner(self, acl_str_owner):
        acl = nodes.Acl.from_str(acl_str_owner)

        assert acl.is_owner()

    def test_is_owner_group(self, acl_str_owner_group):
        acl = nodes.Acl.from_str(acl_str_owner_group)

        assert acl.is_owner_group()

    def test_is_mask(self, acl_str_mask):
        acl = nodes.Acl.from_str(acl_str_mask)

        assert acl.is_mask()

    def test_is_other(self, acl_str_other):
        acl = nodes.Acl.from_str(acl_str_other)

        assert acl.is_other()
