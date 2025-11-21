from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, BlobLeaseClient
from oneflow.core.schemas import ParentModel
from typing import Optional
from pydantic import Field, model_validator, ConfigDict
from oneflow.framework.exception.connector import (
    AzureBlobCredentialsError,
    AzureBlobConnectorInitError,
    AzureBlobLockAlreadyExists,
)
from oneflow.core.utils import create_parent_folder
import os
import io
import uuid
import logging

logging.getLogger().setLevel(logging.DEBUG)

DEFAULT_AZURE_BLOB_LOCK_KEY_NAME = "leaseid"


class AzureBlobCredentials(ParentModel):
    storage_account_name: str = Field(
        ..., description="storage account name to connect the azure service"
    )
    container_name: str = Field(
        ..., description="storage account container where azure service to be connected"
    )
    key: str = Field(
        None,
        description="storage blob name where azure service to be connected and retreive the data",
    )
    client_id: Optional[str] = Field(
        None, description="client id for autenticating to azure service"
    )
    client_secret: Optional[str] = Field(
        None, description="client id for autenticating to azure service"
    )
    tenant_id: Optional[str] = Field(
        None, description="tenant id for autenticating to azure service"
    )
    sas_token: Optional[str] = Field(
        None, description="sas token for autenticating to azure blob service"
    )
    access_key: Optional[str] = Field(
        None, description="account key for autenticating to azure blob service"
    )
    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def validate(self):
        if (
            (
                (self.client_id is None)
                and (self.client_secret is None)
                and (self.tenant_id is None)
            )
            and (self.access_key is None)
            and (self.sas_token is None)
        ):
            raise AzureBlobCredentialsError(
                "Provided credentials are missing required values. Please provide either (client_id,client_secret,tenant_id) or (account_key) or (sas_token) "
            )
        return self


class AzureBlobConnector:
    """This is the class definition for the azure blob connector which connects to azure blob service and helps in fetching the blob,"""

    def __init__(self, credentials: AzureBlobCredentials):
        """This method builds the azure blob client from provided credentials"""
        self.client = None
        self.account_url = (
            f"https://{credentials.storage_account_name}.blob.core.windows.net"
        )
        client_initialization_err = {}
        if credentials.sas_token is not None:
            try:
                self.client = BlobServiceClient(
                    account_url=self.account_url, credential=credentials.sas_token
                )
            except Exception as e:
                client_initialization_err["sas_token"] = e

        if credentials.access_key is not None:
            try:
                if self.client is None:
                    self.client = BlobServiceClient(
                        account_url=self.account_url, credential=credentials.access_key
                    )
            except Exception as e:
                client_initialization_err["access_key"] = e
        if (
            (credentials.client_id is not None)
            and (credentials.client_secret is not None)
            and (credentials.tenant_id is not None)
        ):
            os.environ["AZURE_CLIENT_ID"] = credentials.client_id
            os.environ["AZURE_CLIENT_SECRET"] = credentials.client_secret
            os.environ["AZURE_TENANT_ID"] = credentials.tenant_id
        try:
            if self.client is None:
                client_credentials = DefaultAzureCredential(
                    exclude_visual_studio_code_credential=True,
                    exclude_developer_cli_credential=True,
                    exclude_interactive_browser_credential=True,
                )
                self.client = BlobServiceClient(
                    account_url=self.account_url, credential=client_credentials
                )
        except Exception as e:
            error = []
            if "sas_token" in client_initialization_err:
                error.append(
                    f"Tried with sas token failed with {client_initialization_err['sas_token']}"
                )
            if "access_key" in client_initialization_err:
                error.append(
                    f"Tried with sas token failed with {client_initialization_err['access_key']}"
                )
            error.append(f"Tried with default credentials failed with {e}")

            raise AzureBlobConnectorInitError(
                f"Initializing the azure blob client failed due to error {','.join(error)}"
            )

    def blob_exists(self, container, blob_name):
        """This method checks whether blob exists or not"""

        blob_client = self.client.get_blob_client(container=container, blob=blob_name)

        return blob_client.exists()

    def create_empty_blob(self, container: str, blob_name: str, lease_id: str = None):
        """This method creates empty blob with the provided container and blob name"""
        blob_client = self.client.get_blob_client(container=container, blob=blob_name)

        if not blob_client.exists():
            if lease_id is not None:
                blob_client.upload_blob(io.BytesIO(), lease=lease_id)
            else:
                blob_client.upload_blob(io.BytesIO())

    def get_lock_info_on_blob(
        self,
        blob_client: BlobClient,
        lock_key_name: str = DEFAULT_AZURE_BLOB_LOCK_KEY_NAME,
        lock_id: str = None,
    ):
        """This is the method which gets the lock status on the blob"""
        properties = blob_client.get_blob_properties()
        lease_info = properties.lease
        if (lease_info.status == "locked") and (lock_id is None):
            raise AzureBlobLockAlreadyExists(
                f"Already blob is locked, please break the lease manually on the target blob {properties.container}/{properties.name}"
            )
        metadata = properties.metadata
        return metadata.get(lock_key_name, None)

    def set_lock_info_on_blob(
        self,
        blob_client: BlobClient,
        lock_id: str = None,
        lock_key_name: str = DEFAULT_AZURE_BLOB_LOCK_KEY_NAME,
    ):
        """This method sets the lease id on blob to be communicated by the concurrent peers"""
        properties = blob_client.get_blob_properties()
        lease_info = properties.lease
        metadata = properties.metadata
        if lock_id is None and (lock_key_name in metadata):
            del metadata[lock_key_name]
        else:
            metadata.update({lock_key_name: lock_id})
        if lease_info.status == "locked":
            if lock_id is not None:
                blob_client.set_blob_metadata(metadata, lease=lock_id)
        else:
            blob_client.set_blob_metadata(metadata)

    def lock_blob(
        self,
        container: str,
        blob_name: str,
        lock_key_name: str = DEFAULT_AZURE_BLOB_LOCK_KEY_NAME,
    ):
        """This method gets the lock on the block if can't acquire lock return exception"""
        blob_client = self.client.get_blob_client(container=container, blob=blob_name)
        self.get_lock_info_on_blob(blob_client, lock_key_name=lock_key_name)
        lock_id = uuid.uuid1().hex
        blob_client.acquire_lease(lease_id=lock_id)
        self.set_lock_info_on_blob(blob_client, lock_id, lock_key_name=lock_key_name)
        return lock_id

    def unlock_blob(
        self,
        container: str,
        blob_name: str,
        lock_id: str,
        lock_key_name: str = DEFAULT_AZURE_BLOB_LOCK_KEY_NAME,
    ):
        """This method gets the lock on the block if can't acquire lock return exception"""
        blob_client = self.client.get_blob_client(container=container, blob=blob_name)
        prev_lock_id = self.get_lock_info_on_blob(
            blob_client, lock_key_name=lock_key_name, lock_id=lock_id
        )
        if (prev_lock_id is None) or (prev_lock_id != lock_id):
            raise AzureBlobLockAlreadyExists(
                f"Already Lock exists with different lock {'id'+prev_lock_id if prev_lock_id is not None else 'or incorrect lock and unlock protocol sequence happenend'}"
            )
        lease_client = blob_client.acquire_lease(lease_id=lock_id)
        lease_client.release()
        self.set_lock_info_on_blob(
            blob_client, lock_id=None, lock_key_name=lock_key_name
        )

    def get_blob(
        self,
        container: str,
        blob_name: str,
        local_file_name: str,
        lease_id: str = None,
        safe: bool = True,
    ):
        """This method gets the downloads the blob into local file provided"""
        blob_client = self.client.get_blob_client(container=container, blob=blob_name)

        if not os.path.exists(local_file_name):
            create_parent_folder(local_file_name, file=True)

        if not blob_client.exists():
            if safe:
                with open(local_file_name, "wb") as f:
                    pass
                self.create_empty_blob(container, blob_name, lease_id)
            else:
                raise FileNotFoundError(
                    f"Azure Blob not found at provided location {blob_client.container_name}/{blob_client.blob_name}"
                )
        else:
            with open(local_file_name, "wb") as f:
                data = (
                    blob_client.download_blob()
                    if lease_id is None
                    else blob_client.download_blob(lease=lease_id)
                )
                f.write(data.readall())

    def put_blob(
        self, container: str, blob_name: str, local_file_name: str, lease_id: str = None
    ):
        """This method gets the uploads the blob into azure blob file provided"""
        blob_client = self.client.get_blob_client(container=container, blob=blob_name)

        if not os.path.exists(local_file_name):
            raise FileNotFoundError(
                f"Provided local file {local_file_name} to upload doesn't exists"
            )
        metadata = {}
        if blob_client.exists():
            metadata = blob_client.get_blob_properties().metadata

        with open(local_file_name, "rb") as f:
            data = f.read()
            (
                blob_client.upload_blob(data=data, overwrite=True, metadata=metadata)
                if lease_id is None
                else blob_client.upload_blob(
                    data=data, overwrite=True, lease=lease_id, metadata=metadata
                )
            )
