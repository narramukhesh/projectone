from cdktf import TerraformStack
from constructs import Construct
from oneflow.core.schemas.deploy import InfraStateBackendConfig, InfraStateBackendType
from oneflow.core.schemas.deploy import DeployConfig
import importlib

MODULE_NAME = "oneflow.core.deploy.terraform"


class TerraformComponent(TerraformStack):
    """This class is a implementation of the Databricks Terraform job stack"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        backend_config: InfraStateBackendConfig = None,
    ):
        """This is the initializing class which initializes the terraform project stack"""
        super().__init__(scope, id)

        if backend_config is not None and isinstance(
            backend_config, InfraStateBackendConfig
        ):
            backend_state_class = backend_config.get_terraform_class_obj()
            params = {"scope": self}
            if backend_config.type == InfraStateBackendType.local:
                params["path"] = backend_config.configuration.get("path", None)
            backend_state_class(**params)
        self.components = {}

    def add_components(self, stack_name: str, config: DeployConfig):
        """
        This method add the components to terraform stack

        Parameters
        ------------------------
        stack_name: str
            stack name to be instantiated
        config: DeployConfig
            configuration to be passed to the stack object which was instantiated
        """
        stack_name = stack_name.lower()
        stack_class = stack_name.lower().capitalize() + "Stack"
        stack_module = importlib.import_module(f"{MODULE_NAME}.{stack_name}")

        stack_class_obj = getattr(stack_module, stack_class)

        self.components[stack_name] = self.components.get(stack_name, []) + [
            stack_class_obj(scope=self, config=config)
        ]
        return self
