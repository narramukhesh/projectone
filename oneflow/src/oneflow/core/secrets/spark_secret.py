from oneflow.core.secrets import SecretManager
from oneflow.core.utils.spark import is_in_databricks_runtime
from pyspark.sql import SparkSession
from functools import partial
import json
from oneflow.core.exception.validation import SecretManagerInitializationError
from oneflow.core.exception.execution import SecretManagerFetchFailedError

SECRETS_PATTERN = r"\{\{[\w\-]+/[\w\-]+\}\}"


class SparkSecretManager(SecretManager):
    """This is the spark secret manager implementation"""

    def __init__(self, spark: SparkSession, scope: str = None, secret_file: str = None):
        """This method is initialization for secret manager"""
        self._type = ""
        self._default_scope = scope
        if is_in_databricks_runtime():
            try:
                from pyspark.dbutils import DBUtils

                dbutils = DBUtils(spark)
                self.manager = (
                    partial(dbutils.secrets.get, scope=scope)
                    if scope is not None
                    else partial(dbutils.secrets.get)
                )
                self._type = "Databricks"
            except Exception as e:
                raise SecretManagerInitializationError(
                    f"Problem with setting up the databricks secrets manager setup failed with because of error {e}"
                )

        elif secret_file is not None:
            try:
                with open(secret_file, "rb") as f:
                    secrets = json.load(f)
                    self.manager = lambda scope, key: secrets.get(key, None)
                    self._type = "File"
            except Exception as e:
                raise SecretManagerInitializationError(
                    f"Problem with parsing the secrets file failed with because of error {e}"
                )

    def __str__(self):
        """This called for every str invocation on secret object"""

        return f"{self._type.capitalize()} secret manager {'is created with default scope'+self._default_scope if self._default_scope else ''}"

    def resolve(self, scope: str, key: str) -> str:
        """
        This method is used for the resolving the secrets provided by the key and value

        Parameters
        ------------------
        scope:str
            secret scope name for the secret manager to fetch the secrets from databricks/local scope
        key: str
            secret scope key to retrived from databricks/local scope

        Returns
        ------------------
        str
            returns the value from secret scope
        """
        try:
            value = self.manager(scope=scope, key=key)
            if value is None:
                raise SecretManagerFetchFailedError(
                    f"Secret value doesn't exist for the key {key}"
                )
            return value
        except Exception as e:
            raise SecretManagerFetchFailedError(
                f"Failed to get the secret value for key {key} with error {e} "
            )
