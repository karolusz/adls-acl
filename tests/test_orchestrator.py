import pytest
from adls_acl import orchestrator as o
from adls_acl.nodes import Acl, Node
import azure.storage.filedatalake
import azure.identity
from unittest.mock import call


def test_processor_selector():
    pass


def test__get_service_client_token_credential():
    sc = o._get_service_client_token_credential("test")

    assert isinstance(
        sc, azure.storage.filedatalake._data_lake_service_client.DataLakeServiceClient
    )
    assert sc.account_name == "test"


@pytest.fixture
def mock_client(mocker):
    """Mock DataLakeDirectoryClient"""
    mock_client = mocker.patch(
        "azure.storage.filedatalake.DataLakeDirectoryClient",
        autospec=True,
    )
    mock_client.get_access_control.return_value = {"acl": "user::rwx,user:xxxx:rwx"}

    return mock_client


@pytest.fixture
def test_acl_set():
    return {Acl.from_str("user::rwx"), Acl.from_str("user:xxxx:rwx")}


@pytest.fixture
def test_node():
    parent_node = Node("test1", None)
    child_node = Node("test2", None)
    parent_node.add_child(child_node)

    return parent_node


def test__get_current_acls(mock_client):
    current_acls = o._get_current_acls(mock_client)

    assert len(current_acls) == 2


def test__filter_acls_to_preserve(test_acl_set):
    acls_to_preserve = o._filter_acls_to_preserve(test_acl_set)

    assert len(acls_to_preserve) == 1


def test__set_acls(mock_client, test_acl_set):
    o._set_acls(mock_client, test_acl_set)
    calls = [call(acl=x) for x in test_acl_set]
    mock_client.set_access_control.assert_has_calls(calls, any_order=True)


def test__pushdown_acls(test_node, test_acl_set):
    o._pushdown_acls(test_node, test_acl_set)
    assert test_node.children[0].acls == test_acl_set
