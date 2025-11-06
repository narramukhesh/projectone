from oneflow.core.schemas import ParentModel, ParentEnum
from pydantic import Field, model_validator, ConfigDict
from oneflow.core.schemas.input import SparkInput
from oneflow.core.schemas.output import SparkOutput
from oneflow.core.schemas.execution import SparkExecution
from oneflow.core.schemas.refresh import TaskRefreshPolicy as SparkTaskRefreshPolicy
from typing import Optional, List, Dict, Union, Any
from datetime import datetime
from oneflow.core.exception.deploy import (
    PipelineTaskLibraryResolutionError,
    PipelineConfigurationError,
    SparkTaskConfigurationError,
    TerraformBackendTypeNotSupported,
)
from oneflow.core.schemas.data_objects import Schema, Table, View, Volume, DataObject
import oneflow.core as core


class PipelineRefreshPolicy(ParentModel):
    """This is a schema definition for Pipeline Refresh Policy of Databricks Job"""

    cron_expression: Optional[str] = Field(
        "0 0 0 * * ?", description="This is a cron expression for defining the schedule"
    )
    timezone_id: Optional[str] = Field(
        "UTC", description="This is a Time Zone Expression for schedule of the pipeline"
    )
    status: Optional[str] = Field(
        "UNPAUSED",
        description="This is a status flag of the pipeline whether it is paused/unpaused",
    )


class PipelineCluster(ParentModel):
    """This class is a schema definition for Job/Task Cluster for the pipeline"""

    auto_scale: Optional[bool] = Field(True, description="Auto scaling of the cluster")
    min_workers: Optional[int] = Field(
        1, description="Min workers for the cluster in auto-scale mode"
    )
    max_workers: Optional[int] = Field(
        3, description="Max workers for the cluster in auto-scale mode"
    )
    tags: Optional[Dict[str, str]] = Field(
        None,
        description="tags to define map key/value pair for configuration of the cluster",
    )


class SparkPipelineCluster(PipelineCluster):
    """This class is a schema definition for Job/Task Cluster for the pipeline"""

    spark_version: Optional[str] = Field(
        "3.5.0", description="spark version to be defined for the cluster"
    )
    driver_memory: Optional[int] = Field(
        14, description="driver memory required for the cluster"
    )
    executor_memory: Optional[int] = Field(
        14, description="executor memory required for the cluster"
    )
    driver_cores: Optional[int] = Field(
        4, description="driver cores required for the cluster"
    )
    executor_cores: Optional[int] = Field(
        4, description="executor cores required for the cluster"
    )
    photon: Optional[bool] = Field(
        False, description="Photon enable at the cluster level"
    )
    governance_compute_kind: Optional[str | None] = Field(
        None, description="governance kind for user code isolation"
    )
    lts: Optional[bool] = Field(
        True,
        description="life time support cluster for specific spark version, if enabled cluster will be choosen with LTS",
    )


class PipelineTaskTypes(ParentEnum):
    """This class is a schema definition for possible values Pipeline task types"""

    spark_task = "spark_task"
    spark_pipeline_task = "spark_pipeline_task"
    confluent_connector_task = "confluent_connector_task"


class PipelineTypes(ParentEnum):
    """This class is a schema definition for possible values Pipeline types"""

    spark = "spark"
    confluent = "confluent"


class SparkTaskLibraries(ParentModel):
    """This is the schema definition of the databricks job tasks libraries"""

    type: str = Field("pypi", description="package type name of spark task")
    package: str = Field("oneflow", description="package")
    exclusion: Optional[str] = Field(
        None, description="package type name of spark task"
    )
    repository: Optional[str] = Field(
        core.PROJECT_PACKAGE_URL,
        description="package repository of the spark task libraries",
    )

    @property
    def is_default(self):
        return (
            (self.type == "pypi")
            and (self.package == "oneflow")
            and (self.repository == core.PROJECT_PACKAGE_URL)
        )

    @model_validator(mode="after")
    def validate(self):
        try:
            assert self.type in ["maven", "pypi", "whl"]
        except AssertionError:
            PipelineTaskLibraryResolutionError(
                "Provided type not matches with supported library type"
            )
        return self

    @property
    def get_library(self):
        """This property returns the json version capatible for databricks library"""
        if self.type == "pypi":
            return {"pypi": {"package": self.package, "repo": self.repository}}
        elif self.type == "maven":
            return {
                "maven": {
                    "coordinates": self.package,
                    "repo": self.repository,
                    "exclusions": (
                        self.exclusion.split(",") if self.exclusion is not None else []
                    ),
                }
            }
        elif self.type == "whl":
            return {"whl": self.package}
        else:
            return {}


class TaskConfig(ParentModel):
    """This is a schema definition for the task"""

    name: str = Field(..., description="name of the task")
    description: Optional[str] = Field("", description="description of the task")
    depends_on: Optional[List[str]] = Field(
        None, description="Task which depends on the tasks"
    )
    type: PipelineTaskTypes = Field(..., description="task configuration type")
    model_config = ConfigDict(extra="allow")


class SparkPipelineTask(TaskConfig):
    """Spark pipeline Task for the pipeline where spark task defined as pipeline configuration"""

    type: PipelineTaskTypes = Field(
        PipelineTaskTypes.spark_pipeline_task, description="task configuration type"
    )
    pipeline_name: Optional[str] = Field(
        None, description="pipeline name where current pipeline task refers"
    )
    pipeline_id: Optional[int] = Field(
        None, description="pipeline id where current pipeline task refers"
    )
    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate(self):
        """This method validates whether configured type is spark pipeline type or not"""
        if self.type != PipelineTaskTypes.spark_pipeline_task:
            raise SparkTaskConfigurationError(
                'Provided task type is not spark-pipeline-type, type should be provided as "type": "spark_pipeline_task" as json key-value pair'
            )
        if (self.pipeline_name is None) and (self.pipeline_id is None):
            raise SparkTaskConfigurationError(
                "Not provided pipeline id/name where spark pipeline task depends on"
            )
        return self


class SparkTask(TaskConfig):
    """Spark Task for the pipeline"""

    refresh_policy: Optional[SparkTaskRefreshPolicy] = Field(
        SparkTaskRefreshPolicy(),
        description="Refresh policy defined for the spark pipeline task",
    )

    input: List[SparkInput] = Field(
        ..., description="list of input configurations to be specified"
    )
    output: List[SparkOutput] = Field(
        ..., description="list of output configurations to be specified"
    )
    execution: SparkExecution = Field(
        ..., description="execution function string to be specified"
    )
    cluster: Optional[str] = Field(
        None, description="cluster name specified in the clusters section"
    )
    existing_cluster_id: Optional[str] = Field(
        None, description="existing cluster id on which task will be running"
    )
    type: PipelineTaskTypes = Field(
        PipelineTaskTypes.spark_task, description="task configuration type"
    )
    depends_on: Optional[List[str]] = Field(
        None, description="Spark Task which depends on the output"
    )
    extra_libraries: Optional[List[SparkTaskLibraries]] = Field(
        [SparkTaskLibraries()], description="extra libraries used for the task"
    )
    extra_spark_configuration: Optional[Dict[str, str]] = Field(
        None, description="spark configuration to be configured with spark task"
    )
    metadata_location_path: Optional[str] = Field(
        None, description="Spark task state file metadata path"
    )
    secret_file_path: Optional[str] = Field(
        None, description="Spark task secret file location path"
    )
    tags: Optional[Dict[str, str]] = Field(
        None,
        description="tags to define map key/value pair for configuration of the task",
    )
    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate(self):
        """This method validates whether configured type is spark type or not"""
        if self.type != PipelineTaskTypes.spark_task:
            raise SparkTaskConfigurationError(
                'Provided task type is not spark-type, type should be provided as "type": "spark_task" as json key-value pair'
            )
        return self


class InfraStateBackendType(ParentEnum):
    """This class defines all possible values for terraform backend types"""

    azure = "azure"
    local = "local"


class InfraStateBackendConfig(ParentModel):
    """This class defines the terraform backend configuration"""

    type: Optional[InfraStateBackendType] = Field(
        None, description="terraform backend configuration for storing the state"
    )

    configuration: Optional[Dict[str, str]] = Field(
        None, description="terraform backend configuration"
    )

    def get_terraform_class_obj(self):
        """This method gets the class object for the backend configuration"""
        from cdktf import AzurermBackend, LocalBackend

        if self.type == InfraStateBackendType.azure:
            return AzurermBackend
        elif self.type == InfraStateBackendType.local:
            return LocalBackend
        else:
            raise TerraformBackendTypeNotSupported(
                f"Provided terraform backend is not supported, supported options are {InfraStateBackendType.to_list()}"
            )


class PipelineConfig(TaskConfig):
    """This Class defines the pipeline configuration"""

    name: str = Field(f"pipeline_{datetime.now()}", description="pipeline name")
    description: str = Field(
        ...,
        description="Pipeline description to populate in target",
        json_schema_extra={
            "cli_question": "Please Enter the description of the pipeline: ",
            "type": "python",
        },
    )
    type: PipelineTypes = Field(
        ...,
        description="Pipeline type where following tasks will be executed with corresponding configuration",
    )
    tags: Optional[Dict[str, str]] = Field(
        None,
        description="tags to define map key/value pair for configuration of the pipeline",
    )
    depends_on: Optional[List[str]] = Field(
        None, description="pipeline where it can be dependent it can be pipeline/task"
    )


class SparkPipelineConfig(PipelineConfig):
    """This Class defines the spark pipeline configuration"""

    refresh_policy: Optional[PipelineRefreshPolicy] = Field(
        PipelineRefreshPolicy(),
        description="Refresh policy for the pipeline configuration",
        json_schema_extra={
            "cli_question": "When should you want to schedule the pipeline, Please enter the cron_expression: ",
            "type": "pydantic",
            "examples": """{"cron_expression":"{}"}""",
        },
    )
    type: PipelineTypes = Field(
        PipelineTypes.spark,
        description="Pipeline type where following tasks will be executed with corresponding configuration",
    )
    clusters: Optional[Dict[str, SparkPipelineCluster]] = Field(
        None,
        description="Pipeline Task Cluster configuration",
    )
    tasks: Dict[str, Union[SparkTask, SparkPipelineTask]] = Field(
        ..., description="Mapping of pipeline tasks"
    )

    @model_validator(mode="after")
    def validate(self):
        """This class is validation for pipeline configuration"""
        if self.type != PipelineTypes.spark:
            raise PipelineConfigurationError(
                "Spark Pipeline Json provided type is not spark type, please check and correct it"
            )
        if self.clusters is None:
            self.clusters = {self.name: SparkPipelineCluster()}
        for task in self.tasks:
            try:
                assert task == self.tasks[task].name
            except AssertionError:
                raise PipelineConfigurationError(
                    f"Spark Pipeline Json provided is mismatching configuration where task name:{self.tasks[task].name} is not matches with task key:{task}"
                )
        return self


class ResourceLifecycle(ParentModel):
    """Resource lifecycle class definition to be used for the defining the meta arguments for resource deployment"""

    create_before_destroy: Optional[bool] = Field(
        None,
        description="the new replacement object is created first, and the prior object is destroyed after the replacement is created.",
    )
    ignore_changes: Optional[Union[List[str], str]] = Field(
        None,
        description="This is intended to be used when a resource is created with references to data that may change in the future, but should not affect said resource",
    )
    postcondition: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="This is to specify assumptions and guarantees about how resources and data sources operate",
    )
    precondition: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="This is to specify assumptions and guarantees about how resources and data sources operate",
    )
    prevent_destroy: Optional[bool] = Field(
        None,
        description="This can be used as a measure of safety against the accidental replacement of objects that may be costly to reproduce, such as database instances.",
    )
    replace_triggered_by: Optional[List[str]] = Field(
        None,
        description="Replaces the resource when any of the referenced items change. Supply a list of expressions referencing managed resources, instances, or instance attributes.",
    )


class DeployConfig(ParentModel):
    """This class is parent schema definition for the deploy configuration specific to terraform/specific provider"""

    lifecycle: Optional[ResourceLifecycle] = Field(
        None,
        description="lifecycle conditional checks to be used for create/update/delte the resources",
    )


class PipelineArtifacts(ParentModel):
    """This class is schema definition to upload the artifacts to target location"""

    name: str = Field(..., description="unique description to identify the artifacts")
    source_path: str = Field(
        ..., description="""This is th source path location to upload the artifacts"""
    )
    target_path: str = Field(
        ..., description="""Target location to upload the target data file"""
    )
    workspace: Optional[bool] = Field(
        False, description="""Target location to be workspace or dbfs"""
    )


class PipelineArtifactsDeployConfig(PipelineArtifacts, DeployConfig):
    """This class is schema definition to upload the artifacts to target location with deploy config"""


class SchemaDeployConfig(Schema, DeployConfig):
    """This is the schema definition for the schema/database with deploy config"""


class TableDeployConfig(Table, DeployConfig):
    """This is class definition for the table data object with deploy config"""


class ViewDeployConfig(View, DeployConfig):
    """This is class definition for the view data object with deploy config"""


class VolumeDeployConfig(Volume, DeployConfig):
    """This is class definition for the volume data object with deploy config"""


class DataObjectDeployConfig(DataObject, DeployConfig):
    """This is class definition to represent the hierarchy database->tables, views with deploy config"""

    database: SchemaDeployConfig = Field(
        ...,
        description="schema definition for the corresponding schema object",
        alias="schema",
    )
    tables: Optional[List[TableDeployConfig]] = Field(
        [], description="tables to be defined for the target schema object"
    )
    views: Optional[List[ViewDeployConfig]] = Field(
        [],
        description="View to be created target location for the target schema object",
    )
    volumes: Optional[List[VolumeDeployConfig]] = Field(
        [],
        description="volumes created at target location for the target schema object",
    )


class PipelineDeployConfig(PipelineConfig, DeployConfig):
    """This class defines the pipeline deploy configuration"""


class SparkPipelineDeloyConfig(SparkPipelineConfig, DeployConfig):
    """This Class defines the spark pipeline deploy configuration"""

    depends_on: Optional[List[Any]] = Field(
        None,
        description="This is the placeholder field to hold the dependency between different pipelines",
        json_schema_extra={"secret": True},
    )


class DatabricksDeployConfig(DeployConfig):
    """This is the deploy configuration to be used for the deployment"""

    data: Optional[List[DataObjectDeployConfig]] = Field(
        None, description="data object to be deployed in target location"
    )
    pipeline: Optional[List[SparkPipelineDeloyConfig]] = Field(
        None,
        description="pipeline object to be deployed in target deployment environment",
    )
    artifacts: Optional[List[PipelineArtifactsDeployConfig]] = Field(
        None, description="pipeline artifact to upload to the target location"
    )
