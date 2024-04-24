import azure.identity
import pytest
from adls_acl import auth as a

import azure.storage.filedatalake


def test_get_service_client():
    sc = a.get_service_client("test", "default")

    assert isinstance(
        sc, azure.storage.filedatalake._data_lake_service_client.DataLakeServiceClient
    )
    assert sc.account_name == "test"


def test_token_credential_strategy_default():
    method = a.token_credential_strategy("default")()
    assert isinstance(method, azure.identity.DefaultAzureCredential)


def test_token_credential_strategy_err():
    with pytest.raises(ValueError):
        _ = a.token_credential_strategy("WRONG")
