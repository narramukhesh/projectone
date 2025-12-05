from typing import Dict, Optional, Any
from pydantic import Field, model_validator
from projectoneflow.core.schemas import ParentEnum, ParentModel
from projectoneflow.core.utils.patterns import Singleton
from projectoneflow.framework.exception.contract import EnvironmentParseError
import os
import re
import json, uuid
import tempfile
import projectoneflow.core as core
from projectoneflow.framework.utils import is_windows
from projectoneflow.framework.contract.strategy import DeployStrategyTypes


class EnvTypes(ParentEnum):
    local = "local"
    dev = "dev"
    test = "test"
    uat = "uat"
    prod = "prod"


DEFAULT_ENV = EnvTypes.local
ENVIRONMENT_PATTERN = r"\$\{(\w+)\}"


def replace_environment_variables(value, env):
    """This function is used as utility where it replaces the environment with corresponding variables"""
    if value is not None:
        if isinstance(value, list):
            out = []
            for v in value:
                out.append(replace_environment_variables(v, env))
            value = out
        elif isinstance(value, dict):
            out = {}
            for v in value:
                out[v] = replace_environment_variables(value[v], env)
            value = out
        else:
            matches = re.findall(ENVIRONMENT_PATTERN, value)
            for match in matches:
                replace_value = env.get(match, f"${{{match}}}")
                if replace_value is None:
                    replace_value = f"${{{match}}}"
                if len(value) == len(f"${{{match}}}"):
                    value = replace_value
                else:
                    value = value.replace(f"${{{match}}}", str(replace_value))
    return value


class EnvironmentMode(ParentEnum):
    """This is the enum definition for the strategy modes for contract environments"""

    deploy = "deploy"
    run = "run"


class Environment(metaclass=Singleton):
    """This is singleton class where class used for representing the environment variables"""

    def __init__(self):
        try:
            self.OF_TF_DATABRICKS_CLIENT_ID = os.environ.get(
                "OF_TF_DATABRICKS_CLIENT_ID", ""
            )
            self.OF_TF_DATABRICKS_CLIENT_SECRET = os.environ.get(
                "OF_TF_DATABRICKS_CLIENT_SECRET", ""
            )
            self.OF_TF_DATABRICKS_ACCESS_TOKEN = os.environ.get(
                "OF_TF_DATABRICKS_ACCESS_TOKEN", ""
            )
            self.OF_TF_DATABRICKS_WORKSPACE = os.environ.get(
                "OF_TF_DATABRICKS_WORKSPACE", ""
            )
            self.OF_TF_DATABRICKS_DEPLOY_CLUSTER_ID = os.environ.get(
                "OF_TF_DATABRICKS_DEPLOY_CLUSTER_ID", None
            )
            self.OF_TF_BACKEND_CONFIG = json.loads(
                os.environ.get("OF_TF_BACKEND_CONFIG", "{}")
            )
            self.OF_CURRENT_ENV = os.environ.get("OF_CURRENT_ENV", EnvTypes.local.value)
            self.OF_TF_DATABRICKS_CATALOG = os.environ.get(
                "OF_TF_DATABRICKS_CATALOG", ""
            )
            self.OF_DATABRICKS_ARTIFACTS_PATH = os.environ.get(
                "OF_DATABRICKS_ARTIFACTS_PATH", None
            )
            self.OF_DATABRICKS_SECRET_SCOPE = os.environ.get(
                "OF_DATABRICKS_SECRET_SCOPE", None
            )
            self.OF_PRESETTING_NAME_PREFIX = os.environ.get(
                "OF_PRESETTING_NAME_PREFIX", None
            )
            self.OF_PRESETTING_NAME_SUFFIX = os.environ.get(
                "OF_PRESETTING_NAME_SUFFIX", None
            )
            self.OF_PIPELINE_TAGS = json.loads(os.environ.get("OF_PIPELINE_TAGS", "{}"))
            self.OF_PIPELINE_TASK_CHECKPOINT_LOCATION_PREFIX = os.environ.get(
                "OF_PIPELINE_TASK_CHECKPOINT_LOCATION_PREFIX", None
            )
            self.OF_PIPELINE_METADATA_LOCATION = os.environ.get(
                "OF_PIPELINE_METADATA_LOCATION", None
            )
            self.OF_PRESETTING_TAGS = json.loads(
                os.environ.get("OF_PRESETTING_TAGS", "{}")
            )
            deploy_strategy = os.environ.get("OF_DEPLOY_STRATEGY", "terraform")
            self.OF_DEPLOY_STRATEGY = DeployStrategyTypes(
                "terraform"
                if deploy_strategy not in DeployStrategyTypes.to_list()
                else deploy_strategy
            )
            self.OF_DEPLOY_CORE_PACKAGE_TYPE = os.environ.get(
                "OF_DEPLOY_CORE_PACKAGE_TYPE", "pypi"
            )
            self.OF_DEPLOY_CORE_PACKAGE_PATH = os.environ.get(
                "OF_DEPLOY_CORE_PACKAGE_PATH", "projectoneflow"
            )
            self.OF_DEPLOY_CORE_PACKAGE_REPOSITORY = os.environ.get(
                "OF_DEPLOY_CORE_PACKAGE_REPOSITORY", core.PROJECT_PACKAGE_URL
            )
            self.OF_MODE = EnvironmentMode(os.environ.get("OF_MODE", "deploy"))
            self.OF_MODE_RUN_PIPELINE_STATE_PREFIX = os.environ.get(
                "OF_MODE_RUN_PIPELINE_STATE_PREFIX", tempfile.gettempdir()
            )
            self.OF_MODE_RUN_PIPELINE_CLUSTER_ID = os.environ.get(
                "OF_MODE_RUN_PIPELINE_CLUSTER_ID", None
            )
            self.OF_MODE_RUN_PIPELINE_ID = os.environ.get(
                "OF_MODE_RUN_PIPELINE_ID", None
            )
            self.OF_MODE_RUN_LOCAL_SECRET_FILE = os.environ.get(
                "OF_MODE_RUN_LOCAL_SECRET_FILE", None
            )
            self.OF_MODE_RUN_LOCAL_SPARK_CATALOG_LOCATION = os.environ.get(
                "OF_MODE_RUN_LOCAL_SPARK_CATALOG_LOCATION", tempfile.gettempdir()
            )

            self.OF_MODE_DEPLOY_TEMP_LOCAL_STATE_FILE = os.environ.get(
                "OF_MODE_DEPLOY_TEMP_LOCAL_STATE_FILE",
                os.path.join(
                    tempfile.gettempdir(),
                    ".projectoneflow",
                    "deploy",
                    uuid.uuid1().hex,
                    "terraform.tfstate",
                ),
            )
            import_resource_check = os.environ.get(
                "OF_MODE_DEPLOY_IMPORT_RESOURCES", False
            )
            if import_resource_check in ["t", "true"]:
                import_resource_check = True
            else:
                import_resource_check = False

            self.OF_MODE_DEPLOY_IMPORT_RESOURCES = bool(import_resource_check)
            local_task_parallel_check = os.environ.get(
                "OF_MODE_RUN_LOCAL_TASK_PARALLEL", False
            )
            if local_task_parallel_check in ["t", "true"]:
                local_task_parallel_check = True
            else:
                local_task_parallel_check = False
            self.OF_MODE_RUN_LOCAL_TASK_PARALLEL = bool(local_task_parallel_check)
        except Exception:
            raise EnvironmentParseError(
                "Problem with parsing the environment variables, Please check the global variables available for the framework."
            )

    def get_env(self):
        result = {k: v for k, v in self.__dict__.items() if v != "" and v is not None}
        return result


class Presetting(ParentModel):
    """This is the schema definition for the contract presetting to be used for the setting the contract resource definitions"""

    name_suffix: Optional[str] = Field(
        None,
        description="string value to be included at end of the resource name",
    )
    name_prefix: Optional[str] = Field(
        None,
        description="string value to be included at start of the resource name",
    )
    tags: Optional[Dict[str, str]] = Field(
        None,
        description="tags to be included for each resources",
    )

    def __or__(self, value):
        name_prefix = self.name_prefix
        name_suffix = self.name_suffix
        tags = self.tags
        if name_prefix is None:
            name_prefix = value.name_prefix
        if name_suffix is None:
            name_suffix = value.name_suffix
        if tags is None:
            tags = value.tags
        return Presetting(name_prefix=name_prefix, name_suffix=name_suffix, tags=tags)

    def reconfigure(self):
        if Environment().OF_MODE == EnvironmentMode.run:
            self.name_prefix = (
                Environment().OF_PRESETTING_NAME_PREFIX
                if Environment().OF_PRESETTING_NAME_PREFIX is not None
                else self.name_prefix
            )
            self.name_suffix = (
                Environment().OF_PRESETTING_NAME_SUFFIX
                if Environment().OF_PRESETTING_NAME_SUFFIX is not None
                else self.name_suffix
            )
            self.tags = (
                Environment().OF_PRESETTING_TAGS
                if self.tags is None
                else {**Environment().OF_PRESETTING_TAGS, **self.tags}
            )
        else:
            if self.name_prefix is None:
                self.name_prefix = Environment().OF_PRESETTING_NAME_PREFIX
            if self.name_suffix is None:
                self.name_suffix = Environment().OF_PRESETTING_NAME_SUFFIX
            if self.tags is None:
                self.tags = Environment().OF_PRESETTING_TAGS
        env = Environment().get_env()
        self.name_prefix = replace_environment_variables(self.name_prefix, env)
        self.name_suffix = replace_environment_variables(self.name_suffix, env)
        if self.tags is not None:
            for k in self.tags:
                self.tags[k] = replace_environment_variables(self.tags[k], env)


class EnvironmentSchema(ParentModel):
    """This is the schema definition for the environment schema used in this framework"""

    @staticmethod
    def format_environment_variables(local_env, env):
        for key in local_env:
            local_env[key] = replace_environment_variables(local_env[key], env)
        return local_env


class ContractElementEnvironmentSchema(EnvironmentSchema):
    """This the schema definition for the contract element like datasets, pipeline environment"""

    global_variables: Optional[Dict[str, Any]] = Field(
        {}, description="global variables to be used across the project datasets"
    )
    presetting: Optional[Presetting] = Field(
        Presetting(), description="pre-setting to be included for contract assets"
    )

    def set_preset(self):
        self.presetting.reconfigure()


class LocalContextEnvironmentSchema(EnvironmentSchema):
    """This is the schema definition for the local context for pipelines/datasets"""

    variables: Optional[Dict[str, Any]] = Field(
        {},
        description="variables key and value to be used in the local specific json structure like pipeline,dataset,table",
    )


class LocalEnvironment(LocalContextEnvironmentSchema):
    """This class defines the environment definition for the framework"""

    dev: Optional[LocalContextEnvironmentSchema] = Field(
        LocalContextEnvironmentSchema(),
        description="Dev environment local object level variables to be specified",
    )
    test: Optional[LocalContextEnvironmentSchema] = Field(
        LocalContextEnvironmentSchema(),
        description="Test environment local object level variables to be specified",
    )
    uat: Optional[LocalContextEnvironmentSchema] = Field(
        LocalContextEnvironmentSchema(),
        description="User acceptance testing environment local object level variables to be specified",
    )
    prod: Optional[LocalContextEnvironmentSchema] = Field(
        LocalContextEnvironmentSchema(),
        description="Prod environment local object level variables to be specified",
    )
    local: Optional[LocalContextEnvironmentSchema] = Field(
        LocalContextEnvironmentSchema(),
        description="Local environment local object level variables to be specified",
    )

    @model_validator(mode="after")
    def validate(self):
        global_global_env = Environment().get_env()
        # dev env setting
        self.dev.variables = {**self.variables, **self.dev.variables}
        self.dev.variables = self.__class__.format_environment_variables(
            self.dev.variables, env=global_global_env
        )
        # test env setting
        self.test.variables = {**self.variables, **self.test.variables}
        self.test.variables = self.__class__.format_environment_variables(
            self.test.variables, env=global_global_env
        )
        # uat env setting
        self.uat.variables = {**self.variables, **self.uat.variables}
        self.uat.variables = self.__class__.format_environment_variables(
            self.uat.variables, env=global_global_env
        )
        # prod env setting
        self.prod.variables = {**self.variables, **self.prod.variables}
        self.prod.variables = self.__class__.format_environment_variables(
            self.prod.variables, env=global_global_env
        )
        # local env setting
        self.local.variables = {**self.variables, **self.local.variables}
        self.local.variables = self.__class__.format_environment_variables(
            self.local.variables, env=global_global_env
        )

        return self


class ProjectEnvironment(ContractElementEnvironmentSchema):
    """This class defines the environment definition for the framework"""

    dev: Optional[ContractElementEnvironmentSchema] = Field(
        ContractElementEnvironmentSchema(),
        description="Dev environment project level variables to be specified",
    )
    test: Optional[ContractElementEnvironmentSchema] = Field(
        ContractElementEnvironmentSchema(),
        description="Test environment project level variables to be specified",
    )
    uat: Optional[ContractElementEnvironmentSchema] = Field(
        ContractElementEnvironmentSchema(),
        description="User acceptance test environment project level variables to be specified",
    )
    prod: Optional[ContractElementEnvironmentSchema] = Field(
        ContractElementEnvironmentSchema(),
        description="Prod environment project level variables to be specified",
    )
    local: Optional[ContractElementEnvironmentSchema] = Field(
        ContractElementEnvironmentSchema(),
        description="Local environment project level variables to be specified",
    )

    @model_validator(mode="after")
    def validate(self):
        global_global_env = Environment().get_env()
        # dev env setting
        self.dev.global_variables = {
            **self.global_variables,
            **self.dev.global_variables,
        }
        self.dev.global_variables = self.__class__.format_environment_variables(
            self.dev.global_variables, env=global_global_env
        )
        self.dev.presetting = self.dev.presetting | self.presetting
        self.dev.set_preset()
        # test env setting
        self.test.global_variables = {
            **self.global_variables,
            **self.test.global_variables,
        }
        self.test.global_variables = self.__class__.format_environment_variables(
            self.test.global_variables, env=global_global_env
        )
        self.test.presetting = self.test.presetting | self.presetting
        self.test.set_preset()
        # uat env setting
        self.uat.global_variables = {
            **self.global_variables,
            **self.uat.global_variables,
        }
        self.uat.global_variables = self.__class__.format_environment_variables(
            self.uat.global_variables, env=global_global_env
        )
        self.uat.presetting = self.uat.presetting | self.presetting
        self.uat.set_preset()
        # prod env setting
        self.prod.global_variables = {
            **self.global_variables,
            **self.prod.global_variables,
        }
        self.prod.global_variables = self.__class__.format_environment_variables(
            self.prod.global_variables, env=global_global_env
        )
        self.prod.presetting = self.prod.presetting | self.presetting
        self.prod.set_preset()
        # local env setting
        self.local.global_variables = {
            **self.global_variables,
            **self.local.global_variables,
        }
        self.local.global_variables = self.__class__.format_environment_variables(
            self.local.global_variables, env=global_global_env
        )
        self.local.presetting = self.local.presetting | self.presetting
        self.local.set_preset()
        return self


def format_environment_variables(
    source_object: Dict[str, Any], local_env: Dict[str, Any], global_env: Dict[str, Any]
) -> Dict[str, Any]:
    """
    This function formats the enviornment for the source object with local env and global environment

    Paramters
    --------------------
    source_object: Dict[str,Any]
        source object which to be formatted with the environment variables
    local_env: Dict[str,Any]
        local environment dictionary to map the enviornment variables in source object
    global_env: Dict[str,Any]
        global environment dictionary to map the environment variables in source object

    Returns
    -------------------
    Dict[str,Any]
        return the formatted source object
    """
    if local_env is None:
        local_env = {}
    if global_env is None:
        global_env = {}
    global_global_env = Environment().get_env()
    env = {**global_global_env, **global_env}
    env = {**env, **local_env}

    def list_formatting_variables(list_object):
        local_result = []
        for ele in list_object:
            if isinstance(ele, dict):
                local_result.append(
                    format_environment_variables(
                        ele, local_env=local_env, global_env=global_env
                    )
                )
            elif isinstance(ele, str):
                ele = replace_environment_variables(ele, env)
                local_result.append(ele)
            elif isinstance(ele, list):
                local_result.extend(list_formatting_variables(ele))
        return local_result

    for option in source_object.keys():
        key_option = option
        key = re.match(ENVIRONMENT_PATTERN, option)

        if key is not None:
            v = re.findall(ENVIRONMENT_PATTERN, option)[0]
            key_option = env.get(v, key_option)
            source_object[key_option] = source_object.pop(option)

        if isinstance(source_object[key_option], str):
            source_object[key_option] = replace_environment_variables(
                source_object[key_option], env
            )
        elif isinstance(source_object[key_option], dict):
            source_object[key_option] = format_environment_variables(
                source_object[key_option], local_env=local_env, global_env=global_env
            )
        elif isinstance(source_object[key_option], list):
            source_object[key_option] = list_formatting_variables(
                source_object[key_option]
            )

    return source_object
