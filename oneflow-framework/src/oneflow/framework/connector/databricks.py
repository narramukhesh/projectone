from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import NotFound
from oneflow.framework.contract.config import DatabricksServerDetails
from oneflow.framework.contract.env import Environment
from oneflow.framework.connector import Connector


class DatabricksConnector(Connector):

    def __init__(
        self, host: str, client_id: str, client_secret: str, access_token: str
    ):
        self.client = WorkspaceClient(
            host=host,
            client_id=client_id,
            client_secret=client_secret,
            token=access_token,
        )

    @classmethod
    def build(cls, server_details: DatabricksServerDetails):
        """This method is used to build the databricks runner object which is used for running the workflow and its status"""
        access_token = (
            server_details.access_token
            if server_details is not None and server_details.access_token is not None
            else Environment().OF_TF_DATABRICKS_ACCESS_TOKEN
        )
        client_secret = (
            server_details.client_secret
            if server_details is not None and server_details.client_secret is not None
            else Environment().OF_TF_DATABRICKS_CLIENT_SECRET
        )
        client_id = (
            server_details.client_id
            if server_details is not None and server_details.client_id is not None
            else Environment().OF_TF_DATABRICKS_CLIENT_ID
        )
        host = (
            server_details.workspace_url
            if server_details is not None and server_details.workspace_url is not None
            else Environment().OF_TF_DATABRICKS_WORKSPACE
        )

        if (
            (len(client_id) > 0)
            and (client_id is not None)
            and (len(client_secret) > 0)
            and (client_secret is not None)
        ):
            access_token = None
        elif len(access_token) > 0 and (access_token is not None):
            client_id = None
            client_secret = None
        databricks_runner = cls(
            host=host,
            client_id=client_id,
            client_secret=client_secret,
            access_token=access_token,
        )
        return databricks_runner

    def check_schema_exists(self, catalog, schema):
        """This method checks whether schema exists or not"""
        try:
            self.client.schemas.get(full_name=f"{catalog}.{schema}")
            return True
        except NotFound:
            return False

    def check_table_exists(self, catalog, schema, table):
        """This method checks whether table exists or not"""
        if not self.check_schema_exists(catalog, schema):
            return False
        result = self.client.tables.exists(full_name=f"{catalog}.{schema}.{table}")
        return result.table_exists

    def check_volume_exists(self, catalog, schema, volume):
        """This method checks whether schema exists or not"""
        if not self.check_schema_exists(catalog, schema):
            return False
        for v in self.client.volumes.list(catalog_name=catalog, schema_name=schema):
            if v.name == volume:
                return True
        return False
