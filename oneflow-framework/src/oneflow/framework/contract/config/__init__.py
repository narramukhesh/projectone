from oneflow.core.schemas.data_objects import Table, Schema, View, Volume, VolumeFile
from oneflow.core.schemas.deploy import (
    PipelineTypes,
    SparkPipelineConfig,
    SparkTask,
    SparkPipelineTask,
    ResourceLifecycle,
)
from oneflow.core.schemas import ParentModel, ParentEnum
from oneflow.core.exception.deploy import PipelineConfigurationError
from typing import Optional, List, Any, Union, Dict
from pydantic import Field, model_validator
from oneflow.framework.contract.env import (
    LocalEnvironment,
    Environment,
    format_environment_variables,
    ProjectEnvironment,
)
from oneflow.framework.exception.contract import (
    DatabricksCredentialsValidationError,
    DataObjectPatternMismatch,
    SelectedProjectObjectDoesnotExist,
)
import re

DATABRICKS_DEPLOY_ARTIFACT_PATH = "/FileStore/.oneflow"
DATABRICKS_DEPLOY_WORKSPACE_PATH = "/Shared/.oneflow"
TASK_PARSE_PATTERN = r"\$\{tasks\.(.*)\}"
PIPELINE_PARSE_PATTERN = r"\$\{pipelines\.(.*)\}"
DATASET_PARSE_PATTERN = r"\$\{([\w\.]+)\.(.*)\.tables\.(.*)\}"
EXTERNAL_PARSE_PATTERN = r"\$\{([\w\.]+)\.(.*)\.external\.(.*)\}"
EXTERNAL_FILES_VOLUME_NAME = "external"
environment = Environment()


class ContractObjectTypes(ParentEnum):
    """This is the class definition for the object type to differentiate the managed object vs external object"""

    external = "EXTERNAL"
    managed = "MANAGED"


def get_deploy_lifecycle(
    contract_object_type: ContractObjectTypes,
) -> ResourceLifecycle | None:
    """
    This function which returns the contact object type based on the provided contract type

    Parameters
    --------------------
    contract_object_type:ContractObjectTypes
        contract object type where corresponding lifecycle object created

    Returns
    --------------------
    ResourceLifecycle|None
        it can return none or lifecycle object where the resource object specifies how to treat the object
    """
    if contract_object_type == ContractObjectTypes.external:
        return ResourceLifecycle(ignore_changes="all", prevent_destroy=True)
    else:
        return None


class DatabricksServerDetails(ParentModel):
    """This is the databricks server details schema definition"""

    workspace_url: str = Field(
        environment.DF_TF_DATABRICKS_WORKSPACE,
        description="workspace url to which resources to be deployed",
    )
    client_id: Optional[str] = Field(
        environment.DF_TF_DATABRICKS_CLIENT_ID,
        description="client id for the databricks autentication",
    )
    client_secret: Optional[str] = Field(
        environment.DF_TF_DATABRICKS_CLIENT_SECRET,
        description="client secret for the databricks autentication",
    )
    access_token: Optional[str] = Field(
        environment.DF_TF_DATABRICKS_ACCESS_TOKEN,
        description="access token for the databricks autentication",
    )
    catalog: Optional[str] = Field(
        environment.DF_TF_DATABRICKS_CATALOG,
        description="catalog to be used to be deployed in target databricks environment",
    )
    artifact_deploy_path: Optional[str] = Field(
        environment.DF_DATABRICKS_ARTIFACTS_PATH,
        description="The source artifacts deployment path where source files like execution, transform function are deployed",
    )
    pipeline_state_deploy_schema: Optional[str] = Field(
        None,
        description="The schema location where pipeline state is saved",
    )

    @model_validator(mode="after")
    def validate(self):
        """This method is used for the validating providing the credentials"""
        if (
            ((self.access_token is None) or (len(self.access_token) == 0))
            and ((self.workspace_url is None) or (len(self.workspace_url) == 0))
            and (
                any([self.client_id is None, self.client_secret is None])
                or any([len(self.client_id) == 0, len(self.client_secret) == 0])
            )
        ):
            raise DatabricksCredentialsValidationError(
                "Please provide the databricks workspace url or access token or client id/client secret for databricks server autentication"
            )
        return self


class ConfluenceServerDetails(ParentModel):
    """This is the confluence server details schema definition"""

    url: str = Field(
        ..., description="url to which confluence resources to be deployed"
    )
    client_id: str = Field(
        ..., description="client id for the confluence autentication"
    )
    client_secret: str = Field(
        ..., description="client secret for the confluence autentication"
    )
    access_token: str = Field(
        ..., description="access token for the confluence autentication"
    )


class ProviderTypes(ParentEnum):
    """This is the class definition for the provider types"""

    terraform = "terraform"


class DeploySchema(ParentModel):
    """This is the environment schema for the deployment"""

    provider: Optional[ProviderTypes] = Field(
        ProviderTypes.terraform,
        description="This is the provider to which deployment is carried",
    )

    databricks: Optional[DatabricksServerDetails] = Field(
        None, description="databricks server details configuration"
    )
    confluence: Optional[ConfluenceServerDetails] = Field(
        None, description="confluence server details configuration for docs"
    )

    @model_validator(mode="after")
    def validate(self):
        """This method is used to validate the deploy schema"""
        try:
            if self.databricks is None:
                self.databricks = DatabricksServerDetails()
        except Exception:
            return self
        return self

    @classmethod
    def generate_schema_definition(cls):
        """This is a method which is used to generate the schema definition for specified project name"""
        final_result = {}
        final_result["databricks"] = None
        final_result["confluence"] = None
        return final_result


class ConstractSchema(ParentModel):
    """This is schema definition for the contract"""

    name: Optional[str] = Field(None, description="contract name")
    description: Optional[str] = Field(None, description="contract description")
    stackholders: Optional[List[str]] = Field(
        [], description="stackholders involved in this contract"
    )
    dataset: Any = Field(
        ..., description="List of dataset folder paths to be considered in the project"
    )
    pipelines: Any = Field(
        ..., description="List of pipelines to be considered in the project"
    )
    transform: Any = Field(
        ...,
        description="Transform folder where re-usable functions to be included in the project",
    )
    deploy: Optional[DeploySchema] = Field(
        DeploySchema(), description="Target environment deployment server configuration"
    )
    env: Any = Field(
        None,
        description="Project environment variables configurations",
    )


class ProjectContractSchema(ConstractSchema):
    """This is schema definition for the project"""

    name: Optional[str] = Field(None, description="project contract name")
    description: Optional[str] = Field(None, description="project contract description")
    stackholders: Optional[List[str]] = Field(
        [], description="stackholders involved in this project contract"
    )
    dataset: Optional[List[str]] = Field(
        ["dataset"],
        description="List of dataset folder paths to be considered in the project",
    )
    pipelines: Optional[List[str]] = Field(
        ["pipelines"], description="List of pipelines to be considered in the project"
    )
    transform: Optional[List[str]] = Field(
        ["transform"],
        description="Transform folder where re-usable functions to be included in the project",
    )
    deploy: Optional[DeploySchema] = Field(
        DeploySchema(), description="Target environment deployment server configuration"
    )
    env: Optional[ProjectEnvironment] = Field(
        ProjectEnvironment(),
        description="Project environment variables configurations",
    )

    @model_validator(mode="after")
    def validate(self):
        self.env.validate()
        self.deploy.validate()
        return self

    @classmethod
    def generate_schema_definition(cls, project_name: str):
        """This is a method which is used to generate the schema definition for specified project name"""
        final_result = {}
        final_result["name"] = project_name

        final_result["description"] = None

        final_result["stakeholders"] = None

        final_result["dataset"] = ["dataset"]

        final_result["pipelines"] = ["pipelines"]

        final_result["transform"] = ["transform"]

        final_result["deploy"] = DeploySchema.generate_schema_definition()
        return final_result

    @classmethod
    def get_folder_name(cls):
        return ["dataset", "pipelines", "transform"]


class ContractObjectSchema(ParentModel):
    """This is the parent schema object model where objects in contract can be derived"""

    env: Optional[LocalEnvironment] = Field(
        LocalEnvironment(),
        description="This is the environment variables placeholder to be used in the object schema configuration",
    )
    type: Optional[ContractObjectTypes] = Field(
        ContractObjectTypes.managed,
        description="This is the field where contract object is managed which means all state, resource creation/deletion is bound by the contract but if it external it was not bound by the contract",
    )


class SchemaObjectSchema(ContractObjectSchema):
    """This is the schema object schema definition"""

    database: Dict[str, Any] = Field(
        ..., description="schema definition", alias="schema"
    )

    @classmethod
    def cast(cls, schema_object: Dict[str, Any]):
        """This method casts the object to defined type object"""
        schema_object_class = Schema(**schema_object)
        return schema_object_class

    @classmethod
    def generate_schema_definition(cls, schema_name: str):
        """This is a method which is used to generate the schema definition for specified schema name"""
        final_result = {}
        final_result["schema"] = {}

        final_result["schema"]["name"] = schema_name

        final_result["schema"]["comment"] = ""

        return final_result

    @classmethod
    def get_folder_name(cls):
        return ["tables"]


class VolumeObjectSchema(Volume):
    """This is the class definition for the VolumeObject Schema"""


class TableObjectSchemaType(ParentEnum):
    """This is table schema type"""

    table = "table"
    view = "view"


class TableObjectSchema(ContractObjectSchema):
    """This is table object schema definition"""

    table: Dict[str, Any] = Field(..., description="table schema definition")
    table_type: Optional[TableObjectSchemaType] = Field(
        TableObjectSchemaType.table,
        description="table type either table or view to consolidate",
    )

    @classmethod
    def cast(
        cls,
        table_object: Dict[str, Any],
        table_type: Optional[TableObjectSchemaType] = TableObjectSchemaType.table,
    ):
        """This method casts the object to defined type object"""
        if table_type == TableObjectSchemaType.view:
            table_object_class = View(**table_object)
        else:
            table_object_class = Table(**table_object)
        return table_object_class

    @classmethod
    def generate_schema_definition(
        cls,
        table_name: str,
        table_type: Optional[TableObjectSchemaType] = TableObjectSchemaType.table,
    ):
        """This is a method which is used to generate the schema definition for specified schema name"""
        final_result = {}
        final_result["table"] = {}
        if table_type == TableObjectSchemaType.view:
            final_result["table"]["name"] = table_name
            final_result["table"]["comment"] = ""
            final_result["table"]["query"] = ""
            final_result["table_type"] = "view"
        else:
            final_result["table"]["table_name"] = table_name
            final_result["table"]["comment"] = ""
            final_result["table"]["format"] = "delta"
            final_result["table"]["column_schema"] = []
        return final_result


class PipelineObjectSchema(ContractObjectSchema):
    """This is pipeline object schema definition"""

    pipeline: Dict[str, Any] = Field(..., description="pipeline schema definition")

    @classmethod
    def cast(cls, pipeline_object: Dict[str, Any]):
        """This method casts the object to defined type object"""
        pipeline_type = pipeline_object.get("type", PipelineTypes.spark.value)
        if pipeline_type == PipelineTypes.spark.value:
            pipeline_object_class = SparkPipelineConfig(**pipeline_object)
        else:
            raise PipelineConfigurationError(
                f"Provided pipeline type doesn't supported currently, please review. Supported pipeline types {PipelineTypes.to_list()}"
            )

        return pipeline_object_class


class TaskConfigObjectSchema(ParentModel):
    task: Union[SparkTask, SparkPipelineTask] = Field(
        ..., description="task schema definition"
    )


class TaskObjectSchema(ContractObjectSchema):
    """This is task object schema definition"""

    task: Dict[str, Any] = Field(..., description="task schema definition")

    @classmethod
    def cast(cls, task_object: Dict[str, Any]):
        """This method casts the object to defined type object"""
        task_object_class = TaskConfigObjectSchema(task=task_object)
        return task_object_class.task


class SelectObject(ParentModel):
    """This is the select object to select resource from the project contract"""

    pipelines: Optional[List[str]] = Field([], description="pipelines to be selected")
    schemas: Optional[List[str]] = Field([], description="schemas to be selected")
    tables: Optional[List[str]] = Field([], description="tables to be selected")
    views: Optional[List[str]] = Field([], description="views to be selected")
    external_files: Optional[List[str]] = Field(
        [], description="external files to be selected"
    )

    def isdefined(self):
        """This method is used to check any of the object selection filter is defined"""
        if any(
            [
                len(self.pipelines) > 0,
                len(self.schemas) > 0,
                len(self.tables) > 0,
                len(self.views) > 0,
            ]
        ):
            return True
        else:
            return False

    def add(self, key: str, value: str):
        """This method add the value to described key attribute"""
        if key == "pipelines":
            self.pipelines.append(value)
        elif key == "schemas":
            self.schemas.append(value)
        elif key == "tables":

            self.tables.append(value)
        elif key == "views":
            self.views.append(value)

        elif key == "external_files":
            self.external_files.append(value)
        else:
            raise SelectedProjectObjectDoesnotExist(
                f"Provided {key} is not valid selection"
            )
        self.validate()

    @model_validator(mode="after")
    def validate(self):
        """This is validate provided object details"""
        data_object_pattern = r"^[\w]+\.(\w+)$"
        for table in self.tables:
            if not re.match(data_object_pattern, table):
                raise DataObjectPatternMismatch(
                    f"Provided table {table} should be in form <schema.table>"
                )

            table_ = table.split(".")

            if table_[0] not in self.schemas:
                self.schemas.append(table_[0])

        for view in self.views:
            if not re.match(data_object_pattern, table):
                raise DataObjectPatternMismatch(
                    f"Provided view {view} should be in form <schema.view>"
                )

            view_ = view.split(".")

            if view_[0] not in self.schemas:
                self.schemas.append(view_[0])

        for external in self.external_files:
            if not re.match(data_object_pattern, external):
                raise DataObjectPatternMismatch(
                    f"Provided view {external} should be in form <schema.external_file>"
                )

            external_ = external.split(".")

            if external_[0] not in self.schemas:
                self.schemas.append(external_[0])

        return self
