from cdktf import TerraformVariable, TerraformOutput
from constructs import Construct
from cdktf import Token
from cdktf_cdktf_provider_databricks.provider import DatabricksProvider
from cdktf_cdktf_provider_databricks.job import (
    Job,
    JobTask,
    JobTaskPythonWheelTask,
    JobTaskRunJobTask,
    JobSchedule,
    JobJobClusterNewCluster,
    JobJobCluster,
    JobTaskDependsOn,
    JobJobClusterNewClusterAutoscale,
)
from cdktf_cdktf_provider_databricks.data_databricks_spark_version import (
    DataDatabricksSparkVersion,
)
from cdktf_cdktf_provider_databricks.dbfs_file import DbfsFile
from cdktf_cdktf_provider_databricks.file import File
from cdktf_cdktf_provider_databricks.workspace_file import WorkspaceFile
from cdktf_cdktf_provider_databricks.schema import Schema
from cdktf_cdktf_provider_databricks.volume import Volume
from cdktf_cdktf_provider_databricks.sql_table import (
    SqlTable as Table,
    SqlTableColumn as TableColumn,
)
from cdktf_cdktf_provider_databricks.data_databricks_node_type import (
    DataDatabricksNodeType,
)
from projectoneflow.core.schemas.deploy import (
    DatabricksDeployConfig,
    SparkPipelineDeloyConfig,
    PipelineArtifactsDeployConfig,
    DataObjectDeployConfig,
    SparkPipelineConfig,
    SparkPipelineTask,
    PipelineTaskTypes,
)
from typing import List, Union
from functools import partial
from projectoneflow.core.exception.deploy import PipelineTaskDependencyError
from projectoneflow.core.schemas.data_objects import (
    DataObject,
    Schema as SchemaObject,
    Table as TableObject,
    Volume as VolumeObject,
    View as ViewObject,
)
import json
from projectoneflow.core.utils import replace_special_symbols


class DatabricksStack:
    """This class is a implementation of the Databricks Terraform job stack"""

    def __init__(
        self,
        scope: Construct,
        config: DatabricksDeployConfig,
    ):
        """This is the initializing class which initializes the terraform databricks stack"""

        personal_token = TerraformVariable(
            scope, "databricks_token", type="string", default=None
        )
        host = TerraformVariable(scope, "databricks_host", type="string", default=None)
        client_id = TerraformVariable(
            scope, "databricks_client_id", type="string", default=None
        )
        client_secret = TerraformVariable(
            scope, "databricks_client_secret", type="string", default=None
        )

        DatabricksProvider(
            scope,
            "DatabricksProvider",
            host=host.string_value,
            token=personal_token.string_value,
            client_id=client_id.string_value,
            client_secret=client_secret.string_value,
        )

        # artifacts objects deployments
        self.__class__.build_databricks_artifacts(
            scope=scope, artifacts=config.artifacts
        )

        # data objects deployments
        self.__class__.build_databricks_data_objects(
            scope=scope, data_objects=config.data
        )

        # pipeline objects deployments
        self.__class__.build_databricks_pipeline(
            scope=scope, pipeline_config=config.pipeline
        )

    @classmethod
    def build_databricks_artifacts(
        cls, scope: Construct, artifacts: List[PipelineArtifactsDeployConfig]
    ):
        """
        This method uploads the artifacts to databricks location using the pipeline configuration

        Parameters
        ------------------
        scope: Construct
            This will be Databricks Job Stack Scope
        artifacts: List[PipelineArtifactsDeployConfig]
            This will be pipeline artifacts configuration which is used to upload the artifacts to target location
        """
        if (artifacts is not None) and isinstance(artifacts, list):
            for artifact in artifacts:
                if not artifact.workspace:
                    DbfsFile(
                        scope=scope,
                        id_=replace_special_symbols(artifact.name),
                        source=artifact.source_path,
                        path=artifact.target_path,
                        lifecycle=(
                            artifact.lifecycle.to_json()
                            if artifact.lifecycle is not None
                            else None
                        ),
                    )
                else:
                    WorkspaceFile(
                        scope=scope,
                        id_=replace_special_symbols(artifact.name),
                        source=artifact.source_path,
                        path=artifact.target_path,
                        lifecycle=(
                            artifact.lifecycle.to_json()
                            if artifact.lifecycle is not None
                            else None
                        ),
                    )

    @staticmethod
    def get_databricks_data_object_resource_type(data_object: DataObject):
        """This method returns the resource name for the databricks data objects"""
        resources = {}
        if data_object is not None:

            resources["schema"] = (
                f"{Schema.TF_RESOURCE_TYPE}.{data_object.database.name}"
            )

            for table in data_object.tables:
                resources["table"] = resources.get("table", []) + [
                    f"{Table.TF_RESOURCE_TYPE}.{data_object.database.name}_{table.table_name}"
                ]

            for view in data_object.views:
                resources["view"] = resources.get("view", []) + [
                    f"{Table.TF_RESOURCE_TYPE}.{data_object.database.name}_{view.name}"
                ]

            for volume in data_object.volumes:
                resources["volume"] = resources.get("volume", []) + [
                    f"{Volume.TF_RESOURCE_TYPE}.{data_object.database.name}_{volume.name}"
                ]
        return resources

    @staticmethod
    def get_databricks_schema_resource_type(schema_object: SchemaObject):
        """This method returns the resource name for the databricks schema data objects"""
        if schema_object is not None:
            return f"{Schema.TF_RESOURCE_TYPE}.{replace_special_symbols(schema_object.name)}"

    @staticmethod
    def get_databricks_table_resource_type(
        table_object: Union[TableObject, ViewObject],
    ):
        """This method returns the resource name for the databricks table/view data objects"""
        if table_object is not None:
            return f"{Table.TF_RESOURCE_TYPE}.{replace_special_symbols(table_object.schema_name+'_'+table_object.table_name)}"

    @staticmethod
    def get_databricks_view_resource_type(table_object: Union[TableObject, ViewObject]):
        """This method returns the resource name for the databricks table/view data objects"""
        if table_object is not None:
            return f"{Table.TF_RESOURCE_TYPE}.{replace_special_symbols(table_object.schema_name+'_'+table_object.name)}"

    @staticmethod
    def get_databricks_volume_resource_type(volume_object: VolumeObject):
        """This method returns the resource name for the databricks volume data objects"""
        if volume_object is not None:
            return f"{Volume.TF_RESOURCE_TYPE}.{replace_special_symbols(volume_object.schema_name+'_'+volume_object.name)}"

    @staticmethod
    def get_databricks_pipeline_resource_type(pipeline_object: SparkPipelineConfig):
        """This method returns the resource name for the databricks volume data objects"""
        if pipeline_object is not None:
            return f"{Job.TF_RESOURCE_TYPE}.{replace_special_symbols(pipeline_object.name)}"

    @staticmethod
    def get_databricks_artifact_resource_type(
        artifact_object: PipelineArtifactsDeployConfig,
    ):
        """This method returns the resource name for the databricks volume data objects"""
        if artifact_object is not None:
            return f"{WorkspaceFile.TF_RESOURCE_TYPE}.{replace_special_symbols(artifact_object.name)}"

    @staticmethod
    def get_databricks_data_objects_resource_type(data_objects: List[DataObject]):
        """This method returns the resource name for the databricks data objects"""
        resources = []
        if data_objects is not None:
            for data_object in data_objects:
                resources.append(
                    f"{Schema.TF_RESOURCE_TYPE}.{replace_special_symbols(data_object.database.name)}"
                )
                for table in data_object.tables:
                    resources.append(
                        f"{Table.TF_RESOURCE_TYPE}.{replace_special_symbols(data_object.database.name+'_'+table.table_name)}"
                    )

                for view in data_object.views:
                    resources.append(
                        f"{Table.TF_RESOURCE_TYPE}.{replace_special_symbols(data_object.database.name+'_'+view.name)}"
                    )
                for volume in data_object.volumes:
                    resources.append(
                        f"{Volume.TF_RESOURCE_TYPE}.{replace_special_symbols(data_object.database.name+'_'+volume.name)}"
                    )
        return resources

    @staticmethod
    def get_databricks_pipelines_resource_type(
        pipelines: List[SparkPipelineConfig], artifacts: str
    ):
        """This method returns the resource name for the databricks pipelines"""
        resources = []
        if pipelines is not None:
            for pipeline in pipelines:
                resources.append(
                    f"{Job.TF_RESOURCE_TYPE}.{replace_special_symbols(pipeline.name)}"
                )

            resources.append(
                f"{WorkspaceFile.TF_RESOURCE_TYPE}.{replace_special_symbols(artifacts)}"
            )
        return resources

    @classmethod
    def build_databricks_data_objects(
        cls, scope: Construct, data_objects: List[DataObjectDeployConfig]
    ):
        """
        This method builds the databricks job from pipeline configuration

        Parameters
        ------------------
        scope: Construct
            This will be Databricks Job Stack Scope
        data_object: List[DataObjectDeployConfig]
            This will be dataobject deploy configuration which is used to build the data object schema
        """
        catalog = TerraformVariable(
            scope, "databricks_catalog", type="string", default=None
        )
        if data_objects is not None:
            for data_object in data_objects:
                cls.build_databricks_data_object(scope, data_object, catalog)

    @classmethod
    def build_databricks_data_object(
        cls,
        scope: Construct,
        data_object: DataObjectDeployConfig,
        catalog: TerraformVariable,
    ):
        """
        This method builds the databricks job from pipeline configuration

        Parameters
        ------------------
        scope: Construct
            This will be Databricks Job Stack Scope
        data_object: DataObjectDeployConfig
            This will be dataobject deploy configuration which is used to build the data object schema
        """

        if data_object is not None:
            schema = Schema(
                scope=scope,
                id_=replace_special_symbols(data_object.database.name),
                name=data_object.database.name,
                catalog_name=(
                    catalog.string_value
                    if data_object.database.catalog is None
                    else data_object.database.catalog
                ),
                comment=data_object.database.comment,
                storage_root=data_object.database.location,
                properties=data_object.database.tags,
                lifecycle=(
                    data_object.database.lifecycle.to_json()
                    if data_object.database.lifecycle is not None
                    else None
                ),
            )

            for table in data_object.tables:
                columns = []
                for column in table.column_schema:
                    columns.append(
                        TableColumn(
                            name=column.name,
                            type=column.type,
                            comment=column.description,
                            nullable=column.nullable,
                            identity="default" if column.identity else None,
                        )
                    ),
                Table(
                    scope=scope,
                    id_=replace_special_symbols(
                        f"{data_object.database.name}_{table.table_name}"
                    ),
                    name=table.table_name,
                    schema_name=data_object.database.name,
                    catalog_name=(
                        table.catalog
                        if table.catalog is not None
                        else (
                            catalog.string_value
                            if data_object.database.catalog is None
                            else data_object.database.catalog
                        )
                    ),
                    cluster_keys=table.cluster_by,
                    partitions=table.partition_by,
                    storage_location=table.location,
                    comment=table.comment,
                    data_source_format=table.format,
                    properties=table.properties,
                    table_type="MANAGED" if table.location is None else "EXTERNAL",
                    column=columns,
                    depends_on=[schema],
                    lifecycle=(
                        table.lifecycle.to_json()
                        if table.lifecycle is not None
                        else None
                    ),
                )

            for view in data_object.views:
                Table(
                    scope=scope,
                    id_=replace_special_symbols(
                        f"{data_object.database.name}_{view.name}"
                    ),
                    name=view.name,
                    schema_name=data_object.database.name,
                    catalog_name=(
                        view.catalog
                        if view.catalog is not None
                        else (
                            catalog.string_value
                            if data_object.database.catalog is None
                            else data_object.database.catalog
                        )
                    ),
                    comment=view.comment,
                    table_type="VIEW",
                    view_definition=view.query,
                    depends_on=[schema],
                    lifecycle=(
                        view.lifecycle.to_json() if view.lifecycle is not None else None
                    ),
                )

            for volume in data_object.volumes:
                volume_obj = Volume(
                    scope=scope,
                    id_=f"{data_object.database.name}_{volume.name}",
                    name=volume.name,
                    schema_name=data_object.database.name,
                    catalog_name=(
                        volume.catalog
                        if volume.catalog is not None
                        else (
                            catalog.string_value
                            if data_object.database.catalog is None
                            else data_object.database.catalog
                        )
                    ),
                    comment=volume.comment,
                    volume_type=(
                        "MANAGED" if volume.storage_location is None else "EXTERNAL"
                    ),
                    storage_location=volume.storage_location,
                    depends_on=[schema],
                    lifecycle=(
                        volume.lifecycle.to_json()
                        if volume.lifecycle is not None
                        else None
                    ),
                )
                if (
                    (volume.files is not None)
                    and (isinstance(volume.files, list))
                    and (len(volume.files) > 0)
                ):
                    for file in volume.files:
                        File(
                            scope=scope,
                            id_=f"{data_object.database.name}_{volume.name}_{file.name}",
                            source=file.source_path,
                            path=f"{volume_obj.volume_path}/{file.source_file_name}",
                            depends_on=[volume_obj],
                        )

    @classmethod
    def build_databricks_pipeline(
        cls, scope: Construct, pipeline_config: List[SparkPipelineDeloyConfig]
    ):
        """
        This method builds the databricks job from pipeline configuration

        Parameters
        ------------------
        scope: Construct
            This will be Databricks Job Stack Scope
        pipeline_config: List[SparkPipelineDeloyConfig]
            This will be pipeline configurations which is used to build the pipelines in databricks
        """

        if pipeline_config is not None:
            pipelines = {}
            pipelines_result = {}
            for pipeline in pipeline_config:
                pipelines[pipeline.name] = pipeline
            pipeline_names = set(pipelines.keys())
            for pipeline in pipeline_names:
                for task in pipelines[pipeline].tasks:
                    if isinstance(pipelines[pipeline].tasks[task], SparkPipelineTask):
                        if (
                            (pipelines[pipeline].tasks[task].pipeline_id) is None
                            and (
                                pipelines[pipeline].tasks[task].pipeline_name
                                is not None
                            )
                            and (
                                pipelines[pipeline].tasks[task].pipeline_name
                                not in pipeline_names
                            )
                        ):
                            raise PipelineTaskDependencyError(
                                f"Provided {task} dependency in pipeline {pipeline} defined the pipeline_name attributes with no pipeline_id if your intention to use dependent pipelines to be deployed together but provided pipeline name is not exists in pipelines provided list {list(pipeline_names)} "
                            )
                        elif (pipelines[pipeline].tasks[task].pipeline_id) is None and (
                            pipelines[pipeline].tasks[task].pipeline_name is not None
                        ):
                            pipelines[pipeline].depends_on = (
                                [(pipelines[pipeline].tasks[task].pipeline_name, task)]
                                if pipelines[pipeline].depends_on is None
                                else pipelines[pipeline].depends_on
                                + [
                                    (
                                        pipelines[pipeline].tasks[task].pipeline_name,
                                        task,
                                    )
                                ]
                            )
            while True:
                keys_tobe_executed = pipeline_names - set(pipelines_result.keys())
                if len(keys_tobe_executed) == 0:
                    break
                pipelines_queues = []
                for k in keys_tobe_executed:
                    if (pipelines[k].depends_on is None) or (
                        isinstance(pipelines[k], list)
                        and len(pipelines[k].depends_on) == 0
                    ):
                        pipelines_queues.append(pipelines[k])
                    elif (
                        len(
                            set([i[0] for i in pipelines[k].depends_on])
                            - set(pipelines_result.keys())
                        )
                        == 0
                    ):
                        depends_on = []
                        for i in pipelines[k].depends_on:
                            depends_on.append(pipelines_result[i[0]])
                            pipelines[k].tasks[i[1]].pipeline_id = Token.as_number(
                                pipelines_result[i[0]].id
                            )
                        pipelines[k].depends_on = depends_on
                        pipelines_queues.append(pipelines[k])
                for pipeline in pipelines_queues:
                    pipelines_result[pipeline.name] = (
                        cls.build_databricks_workflow_from_pipeline(scope, pipeline)
                    )

    @classmethod
    def build_databricks_workflow_from_pipeline(
        cls, scope: Construct, pipeline_config: SparkPipelineDeloyConfig
    ):
        """
        This method builds the databricks job from pipeline configuration

        Parameters
        ------------------
        scope: Construct
            This will be Databricks Job Stack Scope
        pipeline_config: SparkPipelineConfig
            This will be pipeline configuration which is used to build the pipeline
        """
        pipeline_name = pipeline_config.name
        pipeline_description = pipeline_config.description
        pipeline_tasks_cluster = {}
        tasks = {}
        for cluster in pipeline_config.clusters:
            cluster_config = pipeline_config.clusters[cluster]
            cluster_auto_scale = JobJobClusterNewClusterAutoscale(
                max_workers=cluster_config.max_workers,
                min_workers=cluster_config.min_workers,
            )
            spark_version = DataDatabricksSparkVersion(
                scope=scope,
                id_=replace_special_symbols(f"{pipeline_name}_{cluster}_spark_version"),
                spark_version=cluster_config.spark_version,
                long_term_support=(
                    cluster_config.lts
                    if hasattr(cluster_config, "lts")
                    and (cluster_config.lts is not None)
                    else True
                ),
            )
            driver_node_type = DataDatabricksNodeType(
                scope=scope,
                id_=replace_special_symbols(
                    f"{pipeline_name}_{cluster}_driver_node_type"
                ),
                local_disk=True,
                min_cores=cluster_config.driver_cores,
                min_memory_gb=cluster_config.driver_memory,
                photon_driver_capable=cluster_config.photon,
            )
            worker_node_type = DataDatabricksNodeType(
                scope=scope,
                id_=replace_special_symbols(
                    f"{pipeline_name}_{cluster}_worker_node_type"
                ),
                local_disk=True,
                min_cores=cluster_config.executor_cores,
                min_memory_gb=cluster_config.executor_memory,
                photon_worker_capable=cluster_config.photon,
            )
            jobtaskcluster = JobJobClusterNewCluster(
                spark_version=spark_version.id,
                autoscale=cluster_auto_scale if cluster_config.auto_scale else None,
                num_workers=cluster_config.min_workers,
                node_type_id=worker_node_type.id,
                driver_node_type_id=driver_node_type.id,
                custom_tags=cluster_config.tags,
                runtime_engine="PHOTON" if cluster_config.photon else "STANDARD",
                kind=(
                    "CLASSIC_PREVIEW"
                    if cluster_config.governance_compute_kind is None
                    else cluster_config.governance_compute_kind
                ),
            )
            pipeline_tasks_cluster[cluster] = JobJobCluster(
                job_cluster_key=cluster, new_cluster=jobtaskcluster
            )
        for t in pipeline_config.tasks:
            task = pipeline_config.tasks[t]
            depends_on = None
            if task.depends_on:
                depends_on = []
                for ta in task.depends_on:
                    try:
                        assert ta in pipeline_config.tasks
                        depends_on.append(JobTaskDependsOn(task_key=ta))
                    except AssertionError:
                        PipelineTaskDependencyError(
                            f"Provided {ta} dependency in tasks {t} cannot be resolved"
                        )
            databricks_task = partial(
                JobTask,
                task_key=t,
                description=task.description,
                depends_on=depends_on,
                max_retries=3,
            )
            if task.type == PipelineTaskTypes.spark_pipeline_task:
                tasks[t] = databricks_task(
                    run_job_task=JobTaskRunJobTask(job_id=task.pipeline_id)
                )
            else:
                tasks[t] = databricks_task(
                    python_wheel_task=JobTaskPythonWheelTask(
                        entry_point="task",
                        parameters=[
                            "--task_configuration",
                            json.dumps(task.to_json()),
                            "--task_type",
                            "spark",
                        ],
                        # named_parameters={
                        #     "task_configuration": json.dumps(task.to_json()),
                        #     "task_type": "spark",
                        # },
                        package_name="projectoneflow",
                    ),
                    library=[lib.get_library for lib in task.extra_libraries],
                    job_cluster_key=(
                        task.cluster if task.existing_cluster_id is None else None
                    ),
                    existing_cluster_id=task.existing_cluster_id,
                )
        schedule = JobSchedule(
            quartz_cron_expression=pipeline_config.refresh_policy.cron_expression,
            pause_status=pipeline_config.refresh_policy.status,
            timezone_id=pipeline_config.refresh_policy.timezone_id,
        )
        job = Job(
            scope=scope,
            name=pipeline_name,
            description=pipeline_description,
            job_cluster=list(pipeline_tasks_cluster.values()),
            id_=replace_special_symbols(pipeline_name),
            task=[task for task in tasks.values()],
            schedule=schedule,
            tags=pipeline_config.tags,
            lifecycle=(
                pipeline_config.lifecycle.to_json()
                if pipeline_config.lifecycle is not None
                else None
            ),
        )

        TerraformOutput(
            scope=scope,
            id=replace_special_symbols(f"{pipeline_name}_job_id"),
            value=job.id,
        )

        return job
