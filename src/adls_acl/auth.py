from azure.identity import (
    DefaultAzureCredential,
    AzureCliCredential,
    EnvironmentCredential,
    WorkloadIdentityCredential,
    ManagedIdentityCredential,
    AzurePowerShellCredential,
    AzureDeveloperCliCredential,
)
from azure.core.credentials import AccessToken
from azure.storage.filedatalake import DataLakeServiceClient
from abc import ABC, abstractmethod
from typing import Any


class Credential(ABC):
    @staticmethod
    @abstractmethod
    def close() -> None:
        pass

    @staticmethod
    @abstractmethod
    def get_token() -> AccessToken:
        pass


def get_service_client(
    account_name: str, auth_method: str, **auth_kwargs: Any
) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.core.windows.net"
    token_credential = token_credential_strategy(auth_method)(**auth_kwargs)
    service_client = DataLakeServiceClient(account_url, credential=token_credential)

    return service_client


def token_credential_strategy(auth_method: str) -> Credential:
    strats = {}
    strats["default"] = DefaultAzureCredential
    strats["environment"] = EnvironmentCredential
    strats["workload"] = WorkloadIdentityCredential
    strats["managedid"] = ManagedIdentityCredential
    strats["azurecli"] = AzureCliCredential
    strats["azureps"] = AzurePowerShellCredential
    strats["azuredevcli"] = AzureDeveloperCliCredential

    return strats[auth_method]
