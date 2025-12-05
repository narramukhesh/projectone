from typing import List, Dict, Any, Optional
import os
import re
from pydantic import Field
from projectoneflow.core.schemas import ParentEnum
from projectoneflow.core.schemas.deploy import (
    PipelineTypes,
    PipelineTaskTypes,
    DatabricksDeployConfig,
    DeployConfig,
    InfraStateBackendType,
    InfraStateBackendConfig as TerraformBackendConfig,
    PipelineArtifactsDeployConfig,
    SparkTaskLibraries,
)
from projectoneflow.framework.contract.strategy import (
    DeployStateResult,
    DeployStrategy,
    DeployApplyOutput,
    DeployPlanOutput,
    DeployBuildOutput,
    DeployInitOutput,
)
import shutil
from projectoneflow.core.deploy.terraform import TerraformComponent
from projectoneflow.core.deploy.terraform.databricks import DatabricksStack
from projectoneflow.framework.connector.azure_blob import (
    AzureBlobConnector,
    AzureBlobCredentials,
)
import tempfile
from cdktf import App
import subprocess
import uuid
from projectoneflow.core.schemas.deploy import (
    VolumeDeployConfig,
    SchemaDeployConfig,
    DataObjectDeployConfig,
    ResourceLifecycle,
)
from projectoneflow.framework.contract.config import SelectObject, EXTERNAL_FILES_VOLUME_NAME
from projectoneflow.framework.contract.config import DATABRICKS_DEPLOY_WORKSPACE_PATH
from projectoneflow.framework.exception.deploy import (
    TerraformActionFetchError,
    TerrafromStatePushError,
    TerrafromStatePullError,
)
from projectoneflow.framework.exception.contract import DeployDetailsMissingError
from projectoneflow.framework.contract.env import Environment, EnvironmentMode
from projectoneflow.framework.connector.databricks import DatabricksConnector
import json
from projectoneflow.framework.utils import is_windows_path, remove_color_codes
from projectoneflow.core.schemas.refresh import TaskRefreshTypes as SparkTaskRefreshTypes
from projectoneflow.core.utils import create_parent_folder

environment = Environment()
DEFAULT_STATE_FILE_NAME = "terraform-test.tfstate"
DEFAULT_TERRAFORM_STATE_LOCK_KEY_NAME = "projectoneflowframeworkstatelockid"


class TerraformStateTypes(ParentEnum):
    """Terraform State types"""

    local = "local"
    remote = "remote"


class TerraformStateResult(DeployStateResult):
    type: TerraformStateTypes = Field(
        TerraformStateTypes.local, description="Terraform state where state is stored"
    )
    local_location: str = Field(
        ..., description="Terraform state where state is stored"
    )
    lock_id: Optional[str] = Field(
        None,
        description="lock id if target remote state is using the mechanism for consistency",
    )
    remote_state_location: str = Field(
        ..., description="Terraform location where remote/local state will be stored"
    )
    remote_state_config: Optional[Dict[str, Any]] = Field(
        None,
        description="remote state configuration used for the connecting with remote",
    )


class TerraformState:

    @staticmethod
    def clear_lock(state_result: TerraformStateResult):
        try:
            if state_result is not None and isinstance(
                state_result, TerraformStateResult
            ):
                if state_result.type == TerraformStateTypes.remote:
                    if (state_result.remote_state_config["type"] == "azure") and (
                        state_result.lock_id is not None
                    ):
                        credentials = AzureBlobCredentials(
                            **state_result.remote_state_config["configuration"]
                        )
                        client = AzureBlobConnector(credentials)
                        client.unlock_blob(
                            container=credentials.container_name,
                            blob_name=credentials.key,
                            lock_id=state_result.lock_id,
                            lock_key_name=DEFAULT_TERRAFORM_STATE_LOCK_KEY_NAME,
                        )
        except Exception:
            pass

    @staticmethod
    def pull_state_to_local(
        backend_config: TerraformBackendConfig, local_state_file_location: str
    ):
        """This method pulls the state from remote/local to local directory"""
        try:
            if backend_config.type == InfraStateBackendType.azure:
                if backend_config.configuration.get("key", None) is None:
                    backend_config.configuration["key"] = DEFAULT_STATE_FILE_NAME
                credentials = AzureBlobCredentials(**backend_config.configuration)
                client = AzureBlobConnector(credentials)
                if not client.blob_exists(credentials.container_name, credentials.key):
                    client.create_empty_blob(
                        credentials.container_name, credentials.key
                    )
                lock_id = client.lock_blob(
                    container=credentials.container_name,
                    blob_name=credentials.key,
                    lock_key_name=DEFAULT_TERRAFORM_STATE_LOCK_KEY_NAME,
                )
                client.get_blob(
                    credentials.container_name,
                    credentials.key,
                    local_state_file_location,
                    lease_id=lock_id,
                )
                return TerraformStateResult(
                    type=TerraformStateTypes.remote,
                    local_location=local_state_file_location,
                    lock_id=lock_id,
                    remote_state_location=credentials.key,
                    remote_state_config=backend_config.to_json(),
                )
            elif backend_config.type == InfraStateBackendType.local:
                if not os.path.exists(backend_config.configuration["path"]):
                    create_parent_folder(
                        backend_config.configuration["path"], file=True
                    )
                    with open(backend_config.configuration["path"], "wb"):
                        pass
                if not os.path.exists(local_state_file_location):
                    create_parent_folder(local_state_file_location, file=True)

                shutil.copy(
                    backend_config.configuration["path"], local_state_file_location
                )
                os.chmod(local_state_file_location, mode=777)

                return TerraformStateResult(
                    type=TerraformStateTypes.local,
                    local_location=local_state_file_location,
                    lock_id=None,
                    remote_state_location=backend_config.configuration["path"],
                )
        except Exception as e:
            raise TerrafromStatePullError(
                f"Pulling the terraform state from {backend_config.type.value} to local failed due to {e}"
            )

    @staticmethod
    def push_state_local_to_remote(*, state_result: TerraformStateResult = None):
        """This method is used to push the state from local to remote"""
        if state_result is not None:
            try:
                if state_result.type == TerraformStateTypes.remote:
                    if (state_result.remote_state_config["type"] == "azure") and (
                        state_result.lock_id is not None
                    ):
                        credentials = AzureBlobCredentials(
                            **state_result.remote_state_config["configuration"]
                        )
                        client = AzureBlobConnector(credentials)
                        client.put_blob(
                            credentials.container_name,
                            credentials.key,
                            state_result.local_location,
                            state_result.lock_id,
                        )
                        client.unlock_blob(
                            container=credentials.container_name,
                            blob_name=credentials.key,
                            lock_id=state_result.lock_id,
                            lock_key_name=DEFAULT_TERRAFORM_STATE_LOCK_KEY_NAME,
                        )
                elif state_result.type == TerraformStateTypes.local:
                    shutil.copy(
                        state_result.local_location, state_result.remote_state_location
                    )
            except Exception as e:
                raise TerrafromStatePushError(
                    f"Pushing the terraform state to {state_result.type.value} because of the error failed due to {e}"
                )


class TerraformAction(ParentEnum):
    create = "create"
    update = "update"
    read = "read"
    delete = "delete"
    forget = "forget"
    destroybeforecreate = "destroybeforecreate"
    createbeforedestroy = "createbeforedestroy"
    replace = "replace"
    no_op = "no-op"


def get_terraform_action(action: List[str]):
    """This is function which returns the terraform action"""
    if len(action) > 2:
        raise TerraformActionFetchError(
            "Terraform actions execeeding the current provided limit"
        )

    if len(action) == 2:
        if action[0] == "create" and action[1] == "delete":
            return TerraformAction("createbeforedestroy")
        elif action[0] == "delete" and action[1] == "create":
            return TerraformAction("destroybeforecreate")
        else:
            raise TerraformActionFetchError(
                f"Terraform actions {action} combination currently not supported"
            )
    elif len(action) == 1:
        if action[0] in TerraformAction.to_list():
            return TerraformAction(action[0])
        else:
            raise TerraformActionFetchError(
                f"Terraform actions {action} combination currently not supported"
            )
    else:
        return None


REF_EXTERNAL_PATTERN = r"#ref\(([^,]+,[^,]+),([^,]+),([^,]+),([^,]+),([^,]+)\)"


def replace_ref_external_object(string):
    """This method will returns terraform databricks object"""
    match = re.match(REF_EXTERNAL_PATTERN, string)
    if match is not None:
        file = match.group(4)
        schema_name = match.group(2)
        ref_external_volume_schema = SchemaDeployConfig(name=schema_name)
        ref_external_volume = VolumeDeployConfig(
            name=EXTERNAL_FILES_VOLUME_NAME,
            schema=schema_name,
        )
        ref_external_volume_dataobject = DataObjectDeployConfig(
            schema=ref_external_volume_schema, volumes=[ref_external_volume]
        )

        ref_external_volume_target_object = (
            DatabricksStack.get_databricks_data_object_resource_type(
                ref_external_volume_dataobject
            )
        )

        ref_file = (
            f"${{{ref_external_volume_target_object['volume'][0]}.volume_path}}/{file}"
        )
        return ref_file
    else:
        return string


REF_OBJECT_PATTERN = {REF_EXTERNAL_PATTERN: replace_ref_external_object}


class TerraformDeployStrategy(DeployStrategy):
    """This class is the concrete implementation of the target databricks deploy strategy"""

    def __init__(self, contract):
        """
        Intializes the deployment strategy
        """
        self.contract = contract

    def get_select_object_target(self, select_object: SelectObject = SelectObject()):
        """This method is used for the creating the target which was selected and deployed"""

        dataset_deploy_assets = self.contract.config.dataset.deploy_asset(select_object)
        pipeline_deploy_assets = self.contract.config.pipelines.deploy_asset(
            select_object
        )

        target_resources = []
        if self.contract.config.deploy.databricks is not None:
            target_resources.extend(
                DatabricksStack.get_databricks_data_objects_resource_type(
                    dataset_deploy_assets
                )
            )
            target_resources.extend(
                DatabricksStack.get_databricks_pipelines_resource_type(
                    pipeline_deploy_assets.get(PipelineTypes.spark, {}).values(),
                    self.contract.project_name,
                )
            )

        return target_resources

    def get_terraform_resource(self, deploy_config: DeployConfig):
        """This method is used for the get the resource names which has no lifecycle defined"""
        resource = []
        if isinstance(deploy_config, DatabricksDeployConfig):
            dataset_deploy_assets = deploy_config.data
            for data_asset in dataset_deploy_assets:
                if data_asset.database.lifecycle is None:
                    result = DatabricksStack.get_databricks_schema_resource_type(
                        data_asset.database
                    )
                    if result is not None:
                        resource.append(result)
                for table in data_asset.tables:
                    obj = table.model_copy(deep=True)
                    obj.schema_name = data_asset.database.name
                    if obj.lifecycle is None:
                        result = DatabricksStack.get_databricks_table_resource_type(obj)
                        if result is not None:
                            resource.append(result)
                for view in data_asset.views:
                    obj = view.model_copy(deep=True)
                    obj.schema_name = data_asset.database.name
                    if obj.lifecycle is None:
                        result = DatabricksStack.get_databricks_view_resource_type(obj)
                        if result is not None:
                            resource.append(result)
                for volume in data_asset.volumes:
                    obj = volume.model_copy(deep=True)
                    obj.schema_name = data_asset.database.name
                    if obj.lifecycle is None:
                        result = DatabricksStack.get_databricks_volume_resource_type(
                            obj
                        )
                        if result is not None:
                            resource.append(result)

            for pipeline in deploy_config.pipeline:
                if pipeline.lifecycle is None:
                    result = DatabricksStack.get_databricks_pipeline_resource_type(
                        pipeline
                    )
                    if result is not None:
                        resource.append(result)

            for artifact in deploy_config.artifacts:
                if artifact.lifecycle is None:
                    result = DatabricksStack.get_databricks_artifact_resource_type(
                        artifact
                    )
                    if result is not None:
                        resource.append(result)

        return resource

    def directory_setup(self, terraform_dir: str):
        """This setup step creates the directory in target location"""
        if terraform_dir is None:
            terraform_dir = os.path.join(
                tempfile.gettempdir(), ".projectoneflow", self.contract.project_name
            )
        else:
            terraform_dir = os.path.join(
                os.path.abspath(terraform_dir), ".projectoneflow", self.contract.project_name
            )

        if not os.path.exists(terraform_dir):
            os.makedirs(terraform_dir, mode=777, exist_ok=True)
        return terraform_dir

    def deploy_build(
        self,
        backend_config: TerraformBackendConfig,
        deploy_directory: str,
        select_object: SelectObject = SelectObject(),
        destroy: bool = False,
    ):
        """
        This method helps to build the target resources which are used to implement

        Parameters
        ---------------------
        backend_config: TerraformBackendConfig
            backend configuration to holds the state of the terraform
        deploy_directory: str
            deploy directory where the terraform configuration files are placed
        select_object: SelectObject
            few selection object to be filter from building the target resources
        destroy:bool
            build the terraform object specifically for the destroying the resource

        """
        out, err = None, None
        try:
            terraform_dir = self.directory_setup(terraform_dir=deploy_directory)
            _, tracked_resources = self.build(
                backend_config=backend_config, terraform_dir=terraform_dir
            )

            targets = []
            if select_object.isdefined():
                select_targets = self.get_select_object_target(
                    select_object=select_object
                )
                if (
                    (select_targets is not None)
                    and (len(select_targets) > 0)
                    and destroy
                ):
                    for r in select_targets:
                        if r in tracked_resources:
                            targets.append(r)
                elif (select_targets is not None) and (len(select_targets) > 0):
                    targets = select_targets

                elif environment.OF_MODE == EnvironmentMode.run:
                    targets = select_targets

            elif destroy:
                targets = tracked_resources
            out = DeployBuildOutput(deploy_dir=terraform_dir, targets=targets)
        except Exception as e:
            err = e

        return out, err

    def deploy_initialize(
        self,
        deploy_directory: str,
        backend_config: TerraformBackendConfig = None,
        local_state_file_location: str = None,
        reconfigure: bool = False,
    ):
        """
        This method helps to initialize the built target resources

        Parameters
        ---------------------
        backend_config: TerraformBackendConfig
            backend configuration to holds the state of the terraform
        deploy_directory: str
            deploy directory where the terraform configuration files are placed
        reconfigure: bool
            reconfigure the deployed terraform resources
        """
        out, err = None, None
        state_output = None
        try:
            ###
            # state pull from remote/local state to local path

            if environment.OF_MODE == EnvironmentMode.deploy:
                state_output = TerraformState.pull_state_to_local(
                    backend_config, local_state_file_location
                )
            ###

            init_out, initialization_err, ret_code = self.terraform_init(
                deploy_directory, None, reconfigure
            )

            if ret_code != 0:
                err = initialization_err
                if environment.OF_MODE == EnvironmentMode.deploy and (
                    state_output is not None
                ):
                    TerraformState.clear_lock(state_output)
                return None, err
            out = DeployInitOutput(init_out=init_out, state_out=state_output)
        except Exception as e:
            err = e
            if environment.OF_MODE == EnvironmentMode.deploy and (
                state_output is not None
            ):
                TerraformState.clear_lock(state_output)
            return None, err

        return out, err

    def deploy_plan(
        self,
        deploy_directory: str,
        state_result: TerraformStateResult = None,
        targets: List[str] = [],
        destroy: bool = False,
        only_plan: bool = False,
    ):
        """
        This method helps to initialize the built target resources

        Parameters
        ---------------------
        deploy_directory: str
            deploy directory where the terraform configuration files are placed
        targets: List[str]
            list of the target resource to be deployed
        destroy: bool
            To specify whether to destroy the resources or not
        state_result: TerraformStateResult
            State result from the initialization result
        only_plan: bool
            Only run until the plan stage
        """
        out, err = None, None
        try:
            plan_out_path = os.path.join(deploy_directory, "state")
            if not os.path.exists(plan_out_path):
                os.makedirs(plan_out_path, mode=777, exist_ok=True)

            plan_out_file = os.path.join(plan_out_path, f"{uuid.uuid1().hex}.plan")

            plan_out, plan_err, ret_code = self.terraform_plan(
                deploy_directory, targets, plan_out_file, destroy
            )

            if ret_code != 0:
                if environment.OF_MODE == EnvironmentMode.deploy and (
                    state_result is not None
                ):
                    TerraformState.clear_lock(state_result)
                return None, plan_err

            plan_out, plan_err, ret_code = self.terraform_read_plan(
                deploy_directory, plan_out_file
            )
            if ret_code != 0:
                if environment.OF_MODE == EnvironmentMode.deploy and (
                    state_result is not None
                ):
                    TerraformState.clear_lock(state_result)
                return None, plan_err
            plan_json_out = json.loads(plan_out)

            if (
                environment.OF_MODE_DEPLOY_IMPORT_RESOURCES
                and environment.OF_MODE == EnvironmentMode.deploy
            ):
                analyze_out, analyze_err, ret_code = self.analyze_plan_import_changes(
                    deploy_directory,
                    plan_json_out,
                    environment.OF_MODE_DEPLOY_IMPORT_RESOURCES,
                )
                if ret_code != 0:
                    if environment.OF_MODE == EnvironmentMode.deploy and (
                        state_result is not None
                    ):
                        TerraformState.clear_lock(state_result)
                    return None, analyze_err
                plan_out, plan_err, ret_code = self.terraform_plan(
                    deploy_directory, targets, plan_out_file, destroy
                )

                if ret_code != 0:
                    if environment.OF_MODE == EnvironmentMode.deploy and (
                        state_result is not None
                    ):
                        TerraformState.clear_lock(state_result)
                    return None, plan_err
                plan_out, plan_err, ret_code = self.terraform_read_plan(
                    deploy_directory, plan_out_file
                )
                if ret_code != 0:
                    if environment.OF_MODE == EnvironmentMode.deploy and (
                        state_result is not None
                    ):
                        TerraformState.clear_lock(state_result)
                    return None, plan_err
                plan_json_out = json.loads(plan_out)

            if only_plan:
                TerraformState.clear_lock(state_result)

            out = DeployPlanOutput(plan_file=plan_out_file, plan_json=plan_json_out)

        except Exception as e:
            if environment.OF_MODE == EnvironmentMode.deploy and (
                state_result is not None
            ):
                TerraformState.clear_lock(state_result)
            err = e

        return out, err

    def analyze_plan_import_changes(
        self, deploy_directory: str, plan: Dict[str, Any], to_import: bool = False
    ):
        """This method used to analyze the plan created and import changes"""

        resource_changes = plan.get("resource_changes", None)
        import_changes = []
        databricks_client = None
        if self.contract.config.deploy.databricks:
            databricks_client = DatabricksConnector.build(
                self.contract.config.deploy.databricks
            )
        if (resource_changes is not None) and isinstance(resource_changes, list):
            for change in resource_changes:
                if (
                    get_terraform_action(change["change"].get("actions", []))
                    == TerraformAction.create
                ):
                    if (
                        ("databricks" in change["provider_name"])
                        and (
                            change["type"]
                            in [
                                "databricks_schema",
                                "databricks_sql_table",
                                "databricks_volume",
                            ]
                        )
                        and (databricks_client is not None)
                    ):
                        if change["type"] == "databricks_schema":
                            catalog_name = change["change"]["after"]["catalog_name"]
                            schema_name = change["change"]["after"]["name"]
                            if databricks_client.check_schema_exists(
                                catalog_name, schema_name
                            ):
                                import_changes.append(
                                    (change["address"], f"{catalog_name}.{schema_name}")
                                )
                        elif change["type"] == "databricks_sql_table":
                            catalog_name = change["change"]["after"]["catalog_name"]
                            schema_name = change["change"]["after"]["schema_name"]
                            table_name = change["change"]["after"]["name"]
                            if databricks_client.check_table_exists(
                                catalog_name, schema_name, table_name
                            ):
                                import_changes.append(
                                    (
                                        change["address"],
                                        f"{catalog_name}.{schema_name}.{table_name}",
                                    )
                                )
                        elif change["type"] == "databricks_volume":
                            catalog_name = change["change"]["after"]["catalog_name"]
                            schema_name = change["change"]["after"]["schema_name"]
                            volume_name = change["change"]["after"]["name"]
                            if databricks_client.check_volume_exists(
                                catalog_name, schema_name, volume_name
                            ):
                                import_changes.append(
                                    (
                                        change["address"],
                                        f"{catalog_name}.{schema_name}.{volume_name}",
                                    )
                                )
        out, err = None, None
        if len(import_changes) > 0:

            out = f"Resources {[change[1] for change in import_changes]} existing in target enviornment which are not available in current terraform state file"

            if to_import:
                for change in import_changes:
                    import_out, import_err, ret_code = self.terraform_import(
                        deploy_directory, change[0], change[1]
                    )
                    if ret_code != 0:
                        import_err = f"tried to import the resource exist in target environment but not available in current terraform state but failed due to problem {import_err}"
                        return import_out, import_err, ret_code
        return out, err, 0

    def deploy_apply(
        self,
        deploy_directory: str,
        plan_file: str,
        state_result: TerraformStateResult = None,
        targets: List[str] = [],
        destroy: bool = False,
    ):
        """
        This method helps to initialize the built target resources

        Parameters
        ---------------------
        backend_config: TerraformBackendConfig
            backend configuration to holds the state of the terraform
        deploy_directory: str
            deploy directory where the terraform configuration files are placed
        targets: List[str]
            list of the target resource to be deployed
        state_file_location: str
            State file location where state is saved
        """
        out, err = None, None
        try:
            apply_out, apply_err, ret_code = self.terraform_apply(
                terraform_dir=deploy_directory,
                target=targets,
                plan_file=plan_file,
                destroy=destroy,
            )
            if ret_code != 0:
                if environment.OF_MODE == EnvironmentMode.deploy and (
                    state_result is not None
                ):
                    TerraformState.clear_lock(state_result)
                return None, apply_err

            apply_out, apply_err, ret_code = self.terraform_output(
                deploy_directory, target=targets
            )
            if ret_code != 0:
                if environment.OF_MODE == EnvironmentMode.deploy and (
                    state_result is not None
                ):
                    TerraformState.clear_lock(state_result)
                return None, apply_err
            apply_out_json = json.loads(apply_out)
            out = DeployApplyOutput(deploy_output=apply_out_json)

            ###
            TerraformState.push_state_local_to_remote(state_result=state_result)
            ###
        except Exception as e:
            if environment.OF_MODE == EnvironmentMode.deploy and (
                state_result is not None
            ):
                TerraformState.clear_lock(state_result)
            err = e

        return out, err

    def resolve_references(self, configuration: Dict[str, Any]):
        """This method solves the reference in the configuration"""

        def string_format_variables(str_object):
            for pattern in REF_OBJECT_PATTERN:
                reference_match = re.match(pattern, str_object)
                if reference_match is not None:
                    replacable_string = reference_match.group(0)
                    replaced_string = REF_OBJECT_PATTERN[pattern](replacable_string)
                    str_object = str_object.replace(replacable_string, replaced_string)
            return str_object

        def list_formatting_variables(list_object):
            local_result = []
            for ele in list_object:
                if isinstance(ele, dict):
                    local_result.append(self.resolve_references(ele))
                elif isinstance(ele, str):
                    ele = string_format_variables(ele)
                    local_result.append(ele)
                elif isinstance(ele, list):
                    local_result.extend(list_formatting_variables(ele))
            return local_result

        for option in configuration.keys():
            key_option = option
            if isinstance(configuration[key_option], str):
                configuration[key_option] = string_format_variables(
                    configuration[key_option]
                )
            elif isinstance(configuration[key_option], dict):
                configuration[key_option] = self.resolve_references(
                    configuration[key_option],
                )
            elif isinstance(configuration[key_option], list):
                configuration[key_option] = list_formatting_variables(
                    configuration[key_option]
                )
        return configuration

    def build(self, backend_config: TerraformBackendConfig, terraform_dir: str) -> str:
        """
        This method builds the terraform components which can creates the terraform cdktf build json at provided location

        Parameters
        --------------------------
        backend_config: TerraformBackendConfig
            this is the backend config parameter to be specified for configuring the terraform state remote location
        terraform_dir: str
            the directory where cdktf json build stacks is saved

        Returns
        ---------------------------
        str
            returns the same terraform directory folder where the terraform state is stored
        """
        terraform_deploy_app = App(outdir=terraform_dir)
        tracked_resource = []
        dataset_deploy_assets = self.contract.config.dataset.deploy_asset()
        pipeline_deploy_assets = self.contract.config.pipelines.deploy_asset()
        terraform_component = TerraformComponent(
            scope=terraform_deploy_app,
            id=self.contract.project_name,
            backend_config=backend_config,
        )

        if (
            len(pipeline_deploy_assets[PipelineTypes.spark]) > 0
            and self.contract.config.deploy.databricks is None
        ):
            raise DeployDetailsMissingError(
                "Pipeline has the spark task and missing the databricks deploy details"
            )
        if self.contract.config.deploy.databricks is not None:
            dataset_deployable_assets = []
            self._set_deploy_databricks_environment()
            artifact = self.contract.get_packaged_artifact()
            databricks_artifact = None
            artifact_package = None
            execution_package = None
            pipeline_state_path = (
                environment.OF_MODE_RUN_PIPELINE_STATE_PREFIX
                if is_windows_path(environment.OF_MODE_RUN_PIPELINE_STATE_PREFIX)
                else "/tmp/"
            )

            if environment.OF_MODE != EnvironmentMode.run:
                project_state_schema = SchemaDeployConfig(
                    name=(
                        f"{self.contract.project_name}_{self.contract.environment.value}_state"
                        if self.contract.config.deploy.databricks.pipeline_state_deploy_schema
                        is None
                        else self.self.contract.config.deploy.databricks.pipeline_state_deploy_schema
                    ),
                    comment=f"This schema is a project {self.contract.project_name} state where all projects pipeline logs, state will be stored",
                )
                project_state_pipeline_volume = VolumeDeployConfig(
                    name=f"pipeline_state",
                    schema=f"{self.contract.project_name}_{self.contract.environment.value}_state",
                )
                pipeline_state_object = DataObjectDeployConfig(
                    schema=project_state_schema, volumes=[project_state_pipeline_volume]
                )

                pipeline_target_object = (
                    DatabricksStack.get_databricks_data_object_resource_type(
                        pipeline_state_object
                    )
                )

                pipeline_state_path = (
                    f"${{{pipeline_target_object['volume'][0]}.volume_path}}"
                )

                dataset_deployable_assets = [pipeline_state_object]
            if artifact is not None:
                target_path = (
                    DATABRICKS_DEPLOY_WORKSPACE_PATH
                    + "/"
                    + (
                        self.contract.config.deploy.databricks.artifact_deploy_path
                        + "/"
                        if self.contract.config.deploy.databricks.artifact_deploy_path
                        is not None
                        else ""
                    )
                    + self.contract.environment.value
                    + (
                        "/"
                        if environment.OF_MODE_RUN_PIPELINE_ID is None
                        else f"/{environment.OF_MODE_RUN_PIPELINE_ID}/"
                    )
                    + self.contract.project_name
                    + "/"
                    + f"{os.path.basename(artifact)}"
                )
                databricks_artifact = [
                    PipelineArtifactsDeployConfig(
                        name=self.contract.project_name,
                        source_path=artifact,
                        target_path=target_path,
                        workspace=True,
                    )
                ]
                artifact_package = SparkTaskLibraries(
                    type="whl", package=f"/Workspace/{target_path}"
                )
                execution_package = SparkTaskLibraries(
                    type=environment.OF_DEPLOY_CORE_PACKAGE_TYPE,
                    package=environment.OF_DEPLOY_CORE_PACKAGE_PATH,
                    repository=environment.OF_DEPLOY_CORE_PACKAGE_REPOSITORY,
                )

            for pipeline in pipeline_deploy_assets[PipelineTypes.spark].values():
                if pipeline.type == PipelineTypes.spark:
                    cluster_mapping = {
                        i: c for i, c in enumerate(pipeline.clusters.keys())
                    }
                    total_clusters = len(cluster_mapping)
                    if (environment.OF_MODE == EnvironmentMode.run) and (
                        environment.OF_MODE_RUN_PIPELINE_CLUSTER_ID is not None
                    ):
                        pipeline.clusters = {}
                    for index, task in enumerate(pipeline.tasks.keys()):
                        if (
                            pipeline.tasks[task].type
                            == PipelineTaskTypes.spark_pipeline_task
                        ) and environment.OF_MODE == EnvironmentMode.run:
                            if (pipeline.tasks[task].pipeline_id is None) and (
                                pipeline.tasks[task].pipeline_name
                                in pipeline_deploy_assets[PipelineTypes.spark]
                            ):
                                ref_pipeline_name = pipeline_deploy_assets[
                                    PipelineTypes.spark
                                ][pipeline.tasks[task].pipeline_name].name
                                pipeline.tasks[task].pipeline_name = ref_pipeline_name
                        if pipeline.tasks[task].type == PipelineTaskTypes.spark_task:
                            # assigning the cluster to task when its not assigned and assigning it in sequence
                            if (environment.OF_MODE == EnvironmentMode.run) and (
                                environment.OF_MODE_RUN_PIPELINE_CLUSTER_ID is not None
                            ):
                                pipeline.tasks[task].existing_cluster_id = (
                                    environment.OF_MODE_RUN_PIPELINE_CLUSTER_ID
                                )
                            elif pipeline.tasks[task].cluster is None:
                                current_cluster = cluster_mapping[
                                    index % total_clusters
                                ]
                                pipeline.tasks[task].cluster = current_cluster

                            for index, output in enumerate(pipeline.tasks[task].output):
                                if (
                                    output.features is not None
                                    and output.features.create_data_object_if_not_exists
                                    is not None
                                ):
                                    if (
                                        output.features.create_data_object_if_not_exists.table
                                        is not None
                                    ) and (
                                        output.features.create_data_object_if_not_exists.table.catalog
                                        is None
                                    ):
                                        output.features.create_data_object_if_not_exists.table.catalog = os.environ[
                                            "TF_VAR_databricks_catalog"
                                        ]
                                pipeline.tasks[task].output[index] = output

                            if artifact:
                                pipeline.tasks[task].extra_libraries = [
                                    artifact_package,
                                    execution_package,
                                ] + [
                                    package
                                    for package in pipeline.tasks[task].extra_libraries
                                    if not package.is_default
                                ]

                            task_json = pipeline.tasks[task].to_json()
                            task_json = self.resolve_references(task_json)
                            task_class = pipeline.tasks[task].__class__
                            pipeline.tasks[task] = task_class(**task_json)

                            # metadata state location path modification
                            pipeline.tasks[task].metadata_location_path = (
                                f"{pipeline_state_path}/{pipeline.name}/{pipeline.tasks[task].name}"
                            )

                            # check point location modification
                            if (
                                pipeline.tasks[task].refresh_policy.type
                                == SparkTaskRefreshTypes.stream
                            ):
                                for index, output in enumerate(
                                    pipeline.tasks[task].output
                                ):
                                    out = output.copy()
                                    out.options.checkpointLocation = f"{pipeline_state_path}/{pipeline.name}/{pipeline.tasks[task].name}/checkpoint/{out.name}"
                                    pipeline.tasks[task].output[index] = out

            for data_object in dataset_deploy_assets:

                no_tables = len(data_object.tables)
                for i in range(0, no_tables):
                    data_object.tables[i].location = None
                    # setting the delta.columnMapping to name so rename and drop column features are enabled by default
                    properties = data_object.tables[i].properties
                    properties["delta.columnMapping.mode"] = properties.get(
                        "delta.columnMapping.mode", "name"
                    )
                    data_object.tables[i].properties = properties

                no_volumes = len(data_object.volumes)
                for i in range(0, no_volumes):
                    data_object.volumes[i].storage_location = None

                dataset_deployable_assets.append(data_object)

            databricks_config = DatabricksDeployConfig(
                data=dataset_deployable_assets,
                pipeline=pipeline_deploy_assets[PipelineTypes.spark].values(),
                artifacts=databricks_artifact,
            )
            tracked_resource.extend(self.get_terraform_resource(databricks_config))
            terraform_component.add_components("databricks", databricks_config)

        terraform_deploy_app.synth()

        return terraform_dir, tracked_resource

    def terraform_init(
        self,
        terraform_dir: str,
        backend_configuration: Dict[str, str] = None,
        reconfigure: bool = False,
    ):
        """This method initializes the terraform repository"""
        cmd = ["terraform", "init"]
        if backend_configuration is not None and isinstance(
            backend_configuration, dict
        ):
            for t in backend_configuration:
                cmd.append(f'-backend-config="{t}={backend_configuration[t]}"')

        if reconfigure:
            cmd.append("-reconfigure")
            cmd.append("-upgrade")
        ti_pid = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            cwd=f"{terraform_dir}/stacks/{self.contract.project_name}",
            env=os.environ,
        )
        out, err = ti_pid.communicate()
        ret_code = ti_pid.returncode
        out = remove_color_codes(out.decode())
        err = remove_color_codes(err.decode())
        return out, err, ret_code

    def terraform_state_pull(
        self,
        terraform_dir: str,
        local_state_file: str = None,
    ):
        """This method pull the terraform state file"""
        cmd = ["terraform", "state", "pull"]
        if local_state_file is not None:
            create_parent_folder(local_state_file, file=True)
            state_file = open(local_state_file, "w")
        else:
            state_file = subprocess.PIPE

        ti_pid = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=state_file,
            cwd=f"{terraform_dir}/stacks/{self.contract.project_name}",
            env=os.environ,
        )
        out, err = ti_pid.communicate()
        ret_code = ti_pid.returncode

        if (local_state_file is not None) and (not state_file.closed):
            state_file.close()
        out = out
        err = remove_color_codes(err.decode())
        return out, err, ret_code

    def terraform_state_push(
        self,
        terraform_dir: str,
        local_state_file: str = None,
    ):
        """This method pull the terraform state file"""
        cmd = ["terraform", "state", "push"]
        if local_state_file is not None:
            cmd.append(local_state_file)

        ti_pid = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            cwd=f"{terraform_dir}/stacks/{self.contract.project_name}",
            env=os.environ,
        )
        out, err = ti_pid.communicate()
        ret_code = ti_pid.returncode
        out = remove_color_codes(out.decode())
        err = remove_color_codes(err.decode())
        return out, err, ret_code

    def terraform_plan(
        self,
        terraform_dir: str,
        target: list = None,
        output_file: str = None,
        destroy: bool = False,
        state_file_location=None,
    ):
        """This method initializes the terraform repository"""
        cmd = ["terraform", "plan", "-input=false"]

        for key in os.environ:
            if key.startswith("TF_VAR") and (os.environ[key] is not None):
                value = os.environ[key]
                var_key = key.replace("TF_VAR_", "").lower()

                cmd.extend(["-var", f"{var_key}={value}"])
        if target is not None and isinstance(target, list) and len(target) > 0:
            for t in target:
                cmd.append(f"-target={t}")
        if output_file is not None:
            cmd.append(f"-out={output_file}")
        if state_file_location is not None:
            cmd.extend(["-state", state_file_location])
        if destroy:
            cmd.append(f"-destroy")
        ti_plan = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            cwd=f"{terraform_dir}/stacks/{self.contract.project_name}",
            env=os.environ,
        )
        out, err = ti_plan.communicate()
        ret_code = ti_plan.returncode
        out = remove_color_codes(out.decode())
        err = remove_color_codes(err.decode())
        return out, err, ret_code

    def terraform_read_plan(self, terraform_dir: str, plan_output_file: str = None):
        """This method reads the terraform plan as specified"""
        cmd = ["terraform", "show", "-json", f"{plan_output_file}"]
        ti_plan = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            cwd=f"{terraform_dir}/stacks/{self.contract.project_name}",
            env=os.environ,
        )
        out, err = ti_plan.communicate()
        ret_code = ti_plan.returncode
        out = remove_color_codes(out.decode())
        err = remove_color_codes(err.decode())
        return out, err, ret_code

    def terraform_apply(
        self,
        terraform_dir: str,
        target: list = None,
        plan_file: str = None,
        destroy: bool = False,
        state_file_location=None,
    ):
        """This method initializes the terraform repository"""
        cmd = ["terraform", "apply", "-input=false", "-auto-approve"]
        if state_file_location is not None:
            cmd.extend(["-state", state_file_location])
        if destroy:
            cmd.append(f"-destroy")

        if plan_file is None:
            for key in os.environ:
                if key.startswith("TF_VAR"):
                    value = os.environ[key]
                    var_key = key.replace("TF_VAR_", "").lower()
                    cmd.extend(["-var", f"'{var_key}={value}'"])

        if target is not None and isinstance(target, list) and len(target) > 0:
            for t in target:
                cmd.append(f"-target={t}")

        if plan_file is not None:
            cmd.append(f"{plan_file}")

        ti_apply = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            cwd=f"{terraform_dir}/stacks/{self.contract.project_name}",
            env=os.environ,
        )
        out, err = ti_apply.communicate()
        ret_code = ti_apply.returncode
        out = remove_color_codes(out.decode())
        err = remove_color_codes(err.decode())
        return out, err, ret_code

    def terraform_import(
        self, terraform_dir: str, target_address: str, source_address: str
    ):
        """This method initializes the terraform repository"""
        cmd = ["terraform", "import", target_address, source_address]

        ti_apply = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            cwd=f"{terraform_dir}/stacks/{self.contract.project_name}",
            env=os.environ,
        )
        out, err = ti_apply.communicate()
        ret_code = ti_apply.returncode
        out = remove_color_codes(out.decode())
        err = remove_color_codes(err.decode())
        return out, err, ret_code

    def terraform_output(self, terraform_dir: str, target: list = None):
        """This method initializes the terraform repository"""
        cmd = ["terraform", "output", "-json"]

        ti_apply = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            cwd=f"{terraform_dir}/stacks/{self.contract.project_name}",
            env=os.environ,
        )
        out, err = ti_apply.communicate()
        ret_code = ti_apply.returncode
        out = remove_color_codes(out.decode())
        err = remove_color_codes(err.decode())
        return out, err, ret_code

    def terraform_destroy(self, terraform_dir: str, target: list = None):
        """This method destroy the terraform resources"""

        cmd = ["terraform", "apply", "-destroy", "-input=false", "-auto-approve"]
        if target is not None and isinstance(target, list) and len(target) > 0:
            for t in target:
                cmd.append(f"-target={t}")

        ti_apply = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            cwd=f"{terraform_dir}/stacks/{self.contract.project_name}",
            env=os.environ,
        )
        out, err = ti_apply.communicate()
        ret_code = ti_apply.returncode
        out = remove_color_codes(out.decode())
        err = remove_color_codes(err.decode())

        return out, err, ret_code

    def _set_deploy_databricks_environment(self):
        """Sets the deployment configuration"""

        databricks_credentials = self.contract.config.deploy.databricks

        os.environ["TF_VAR_databricks_token"] = (
            databricks_credentials.access_token
            if databricks_credentials.access_token is not None
            else environment.OF_TF_DATABRICKS_ACCESS_TOKEN
        )
        os.environ["TF_VAR_databricks_client_id"] = (
            databricks_credentials.client_id
            if databricks_credentials.client_id is not None
            else environment.OF_TF_DATABRICKS_CLIENT_ID
        )
        os.environ["TF_VAR_databricks_client_secret"] = (
            databricks_credentials.client_secret
            if databricks_credentials.client_secret is not None
            else environment.OF_TF_DATABRICKS_CLIENT_SECRET
        )
        os.environ["TF_VAR_databricks_host"] = (
            databricks_credentials.workspace_url
            if databricks_credentials.workspace_url is not None
            else environment.OF_TF_DATABRICKS_WORKSPACE
        )
        os.environ["TF_VAR_databricks_catalog"] = (
            databricks_credentials.catalog
            if databricks_credentials.catalog is not None
            else environment.OF_TF_DATABRICKS_CATALOG
        )
