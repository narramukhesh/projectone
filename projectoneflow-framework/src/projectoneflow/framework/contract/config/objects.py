from typing import ClassVar, Optional, List
from abc import ABC
import pathlib
from projectoneflow.framework.contract.env import (
    EnvTypes,
    ContractElementEnvironmentSchema,
    Environment,
    format_environment_variables,
    replace_environment_variables,
    ENVIRONMENT_PATTERN,
    EnvironmentMode,
)
import re
from projectoneflow.core.schemas import cast
from projectoneflow.core.schemas.deploy import (
    PipelineTaskTypes,
    PipelineTypes,
    DeployConfig,
    TableDeployConfig,
    ViewDeployConfig,
    SchemaDeployConfig,
    VolumeDeployConfig,
    DataObjectDeployConfig,
    SparkPipelineDeloyConfig,
)
from projectoneflow.framework.validation.data_objects import TableValidation, SchemaValidation
from projectoneflow.core.schemas.data_objects import DataObject
from projectoneflow.framework.validation import Run, Check, ResultEnum
from projectoneflow.framework.validation.pipeline import (
    REQUIRED_PIPELINE_FOLDERS,
    PipelineValidation,
)
from projectoneflow.core.deploy.terraform.databricks import DatabricksStack
from projectoneflow.framework.contract.config import (
    TableObjectSchema,
    TableObjectSchemaType,
    VolumeObjectSchema,
    SchemaObjectSchema,
    PipelineObjectSchema,
    TaskObjectSchema,
    SelectObject,
    ContractObjectTypes,
    get_deploy_lifecycle,
    TASK_PARSE_PATTERN,
    DATASET_PARSE_PATTERN,
    EXTERNAL_FILES_VOLUME_NAME,
    EXTERNAL_PARSE_PATTERN,
)
from projectoneflow.framework.exception.contract import (
    ProjectContractDatasetNotExists,
    ProjectContractPipelineNotExists,
)
from projectoneflow.core.utils import read_json_file
from projectoneflow.core.utils import NamespaceDict as SimpleNamespace
from typing import Protocol, runtime_checkable
import os


class DeployComponentMapping:
    """
    This is the class definition where single product can be mapped to multiple platforms

    Like spark is supported by the multiple managed platforms like data bricks and like kakfa like confluent.
    where this  class used select the approriate platform
    """

    @property
    def task_type_to_platform(
        self, environment: EnvTypes, pipeline_type: PipelineTypes
    ):
        """This method maps the task type to target platform"""
        if environment != EnvTypes.local:
            if pipeline_type == PipelineTypes.spark:
                return ["databricks"]
            elif pipeline_type == PipelineTypes.confluent:
                return ["confluent"]
        else:
            return "local"


@runtime_checkable
class ContractObject(Protocol):
    """This is the base class for the contract object with single attribute to determine whether object is modifiable by execution environment which is used for the presetting"""

    is_modifiable_by_env: ClassVar[bool] = False

    def validate(
        self, run: Run, select_object: Optional[SelectObject | None] = None
    ) -> None:
        """This is the validate method to run the validation on the data objects"""

    def deploy_asset(
        self, select_object: Optional[SelectObject | None] = None
    ) -> DeployConfig | List[DeployConfig]:
        """This is the deploy asset method where it is used to return the deployable configuration"""


class BaseContractObject(ABC, ContractObject):
    """This is the base class contract object where all contract object will be used"""


class TableContractObject(BaseContractObject):
    """
    This is schema definition for the table contract object
    """

    def __init__(
        self,
        table_location: str,
        env: ContractElementEnvironmentSchema,
        environment: EnvTypes = EnvTypes.local,
    ):
        table_name = os.path.basename(table_location).replace(".json", "")
        self.name = table_name
        self.location = table_location
        self.validation_error = None
        self.type = TableObjectSchemaType.table
        try:
            self.table_config = TableObjectSchema(**read_json_file(table_location))
            table_config = format_environment_variables(
                self.table_config.table,
                getattr(getattr(self.table_config.env, environment.value), "variables"),
                env.global_variables,
            )
            table_config = TableObjectSchema.cast(
                table_config, self.table_config.table_type
            )
            self.config = table_config
            self.type = self.table_config.table_type

        except Exception as e:
            self.validation_error = Check(
                name="check_table_config_schema",
                object_type="table",
                object_name=table_name,
                description="This check validates whether table json configuration is valid or not",
                details=f"Error in parsing the configuration with error {e}",
                result=ResultEnum.error,
                location=table_location,
            )

    def validate(self, run: Run, select_object: SelectObject | None = None) -> None:
        """This is the validate method to run the validation on the data table object"""
        if self.validation_error is None:
            return TableValidation.validate(
                run, self.config, self.name, self.location, table_type=self.type.value
            )
        else:
            run.append(self.validation_error)
            return

    def deploy_asset(
        self, select_object: SelectObject | None = None
    ) -> TableDeployConfig:
        """This method is used to deploy the asset in target enviroment"""
        if self.type == TableObjectSchemaType.view:
            deploy_obj = cast(ViewDeployConfig, self.config)
        else:
            deploy_obj = cast(TableDeployConfig, self.config)
        deploy_obj.lifecycle = get_deploy_lifecycle(self.table_config.type)

        return deploy_obj


class SchemaContractObject(BaseContractObject):
    """This is the class definition for the schema contract object"""

    is_modifiable_by_env: bool = True

    def __init__(
        self,
        schema_config_location: str,
        env: ContractElementEnvironmentSchema,
        environment: EnvTypes = EnvTypes.local,
    ):

        schema_name = os.path.basename(schema_config_location).replace(".json", "")
        schema_location = pathlib.Path(schema_config_location).resolve().parent
        self.name = schema_name
        self.location = schema_location.__str__()
        self.validation_error = None
        self.tables = SimpleNamespace()
        self.views = SimpleNamespace()
        self.external_files = SimpleNamespace()
        self.config = None

        try:
            schema_json = read_json_file(schema_config_location)
            self.schema_config = SchemaObjectSchema(**schema_json)
            schema_config = format_environment_variables(
                self.schema_config.database,
                getattr(
                    getattr(self.schema_config.env, environment.value), "variables"
                ),
                env.global_variables,
            )
            schema_config = SchemaObjectSchema.cast(schema_config)
            schema_name = schema_config.name
            schema_name = (
                schema_name + "_" + env.presetting.name_suffix
                if (
                    env.presetting.name_suffix is not None
                    and len(env.presetting.name_suffix) > 0
                )
                else schema_name
            )
            schema_name = (
                env.presetting.name_prefix + "_" + schema_name
                if (
                    env.presetting.name_prefix is not None
                    and len(env.presetting.name_prefix) > 0
                )
                else schema_name
            )
            schema_config.name = schema_name
            self.config = schema_config

            tables = os.path.join(schema_location.__str__(), "tables")

            if os.path.exists(tables):
                table_list = os.listdir(tables)

                for file in table_list:
                    table_path = os.path.join(tables, file)
                    table = TableContractObject(
                        table_location=table_path, env=env, environment=environment
                    )
                    self.tables[table.name] = table

            views = os.path.join(schema_location.__str__(), "views")

            if os.path.exists(views):
                view_list = os.listdir(views)

                for file in view_list:
                    view_path = os.path.join(views, file)
                    view = TableContractObject(
                        table_location=view_path, env=env, environment=environment
                    )
                    self.views[view.name] = view
            # special handling because external folder is not first-class citizen in this contract specification
            external_files = os.path.join(
                schema_location.__str__(), EXTERNAL_FILES_VOLUME_NAME
            )

            if os.path.exists(external_files):
                external_files_list = os.listdir(external_files)
                files = []
                for file in external_files_list:
                    files.append(
                        {
                            "name": file.split(".")[0],
                            "source_file_name": file,
                            "source_path": os.path.join(external_files, file),
                        }
                    )
                self.external_files[EXTERNAL_FILES_VOLUME_NAME] = VolumeObjectSchema(
                    name=EXTERNAL_FILES_VOLUME_NAME,
                    schema_name=self.config.name,
                    files=files if len(files) > 0 else None,
                )
        except Exception as e:
            self.validation_error = Check(
                name="check_schema_config_schema",
                object_type="schema",
                object_name=schema_name,
                description="This check validates whether schema json configuration is valid or not",
                details=f"Error in parsing the configuration with error {e}",
                result=ResultEnum.error,
                location=schema_config_location,
            )

    def validate(self, run: Run, select_object: SelectObject = SelectObject()) -> None:
        """This is the validate method to run the validation on the data schema object"""
        if self.validation_error is None:
            SchemaValidation.validate(run, self.config, self.name, self.location)

            tables = set(self.tables.keys())
            views = set(self.views.keys())

            if select_object.isdefined():
                if len(select_object.tables) > 0:
                    selected_tables = set(
                        [
                            i.split(".")[-1]
                            for i in select_object.tables
                            if i.split(".")[0] == self.name
                        ]
                    )
                    identified_tables = selected_tables.difference(tables)

                    if len(identified_tables) > 0:
                        tables = set()

                        run.append(
                            Check(
                                name="check_selected_tables",
                                object_type="table",
                                object_name=",".join(list(identified_tables)),
                                description="This checks whether selected tables exists or not",
                                details=f"missing provided tables {','.join(list(identified_tables))}",
                                result=ResultEnum.error,
                                location=None,
                            )
                        )
                    else:
                        tables = selected_tables
                else:
                    tables = set()
                if len(select_object.views) > 0:
                    selected_views = set(
                        [
                            i.split(".")[-1]
                            for i in select_object.views
                            if i.split(".")[0] == self.name
                        ]
                    )
                    identified_views = selected_views.difference(views)

                    if len(identified_views) > 0:
                        views = set()

                        run.append(
                            Check(
                                name="check_selected_views",
                                object_type="views",
                                object_name=",".join(list(identified_views)),
                                description="This checks whether selected views exists or not",
                                details=f"missing provided tables {','.join(list(identified_views))}",
                                result=ResultEnum.error,
                                location=None,
                            )
                        )
                    else:
                        views = selected_views
                else:
                    views = set()
            for table in tables:
                self.tables[table].validate(run)

            for view in views:
                self.views[view].validate(run)
        else:
            run.append(self.validation_error)
        return

    def deploy_asset(
        self, select_object: SelectObject = SelectObject()
    ) -> DataObjectDeployConfig:
        """This method is used to deploy the asset in target environment"""
        schema_deploy_obj = cast(SchemaDeployConfig, self.config)
        schema_deploy_obj.lifecycle = get_deploy_lifecycle(self.schema_config.type)
        data_object = DataObjectDeployConfig(schema=schema_deploy_obj)

        tables = set(self.tables.keys())
        views = set(self.views.keys())

        if select_object.isdefined():
            if len(select_object.tables) > 0:
                selected_tables = set(
                    [
                        i.split(".")[-1]
                        for i in select_object.tables
                        if i.split(".")[0] == self.name
                    ]
                )
                identified_tables = selected_tables.intersection(tables)
                tables = identified_tables
            else:
                tables = set()

            if len(select_object.views) > 0:
                selected_views = set(
                    [
                        i.split(".")[-1]
                        for i in select_object.views
                        if i.split(".")[0] == self.name
                    ]
                )
                identified_views = selected_views.intersection(views)
                views = identified_views
            else:
                views = set()

        deployed_tables = []
        deployed_views = []
        for table in tables:
            deployed_tables.append(self.tables[table].deploy_asset())
        for view in views:
            deployed_views.append(self.views[view].deploy_asset())

        if EXTERNAL_FILES_VOLUME_NAME in self.external_files:
            volume_deploy_config = cast(
                VolumeDeployConfig, self.external_files.external
            )
            data_object.volumes = [volume_deploy_config]
        data_object.tables = deployed_tables
        data_object.views = deployed_views
        return data_object


class DatasetContractObject(BaseContractObject):
    """This is the contract class definition to hold the schema and table object"""

    is_modifiable_by_env = True

    def __init__(
        self,
        project_name: str,
        project_location: str,
        dataset_location: list,
        env: ContractElementEnvironmentSchema,
        environment: EnvTypes = EnvTypes.local,
    ):
        self.validation_error = []
        self.schemas = SimpleNamespace()
        self.project_name = project_name

        for folder in dataset_location:
            folder = os.path.join(project_location, folder)
            if not os.path.exists(folder):
                raise ProjectContractDatasetNotExists(
                    f"Project contract has defined template, where datasets folder {folder} which was provided is missing, please check the project configuration file"
                )
            schemas = os.listdir(folder)

            if len(schemas) == 0:
                self.validation_error.append(
                    Check(
                        name="check_dataset_folder",
                        object_type="dataset",
                        description="This check validates whether schemas folders exists or not",
                        details="No schema folder exists",
                        result=ResultEnum.warning,
                        location=folder,
                    )
                )
            for schema in schemas:
                schema_location = os.path.join(folder, schema)

                schema_config_location = os.path.join(schema_location, f"{schema}.json")

                if not os.path.exists(schema_config_location):
                    self.validation_error.append(
                        Check(
                            name="check_schema_file_exists",
                            object_type="schema",
                            object_name=schema,
                            description="This checks whether schema json configuration file exists or not",
                            details=f"No schema file with name {schema}.json exists",
                            result=ResultEnum.warning,
                            location=schema_config_location,
                        )
                    )
                    continue
                schema_obj = SchemaContractObject(
                    schema_config_location=schema_config_location,
                    env=env,
                    environment=environment,
                )
                self.schemas[schema_obj.name] = schema_obj

    def validate(self, run: Run, select_object: SelectObject = SelectObject()) -> None:
        """This is the validate method to run the validation on the data set object"""
        schemas = set(self.schemas.keys())

        if select_object.isdefined():
            if len(select_object.schemas) > 0:
                selected_schemas = set(select_object.schemas)
                identified_schemas = selected_schemas.difference(schemas)

                if len(identified_schemas) > 0:
                    schemas = set()

                    self.validation_error.append(
                        Check(
                            name="check_selected_schema",
                            object_type="schema",
                            object_name=",".join(list(identified_schemas)),
                            description="This checks whether selected schema exists or not",
                            details=f"missing provided schema {','.join(list(identified_schemas))}",
                            result=ResultEnum.error,
                            location=None,
                        )
                    )
                else:
                    schemas = selected_schemas
            else:
                schemas = set()

        for schema in schemas:
            self.schemas[schema].validate(run, select_object)
        if self.validation_error is not None:
            run.extend(self.validation_error)
        return

    def deploy_asset(self, select_object: SelectObject = SelectObject()):
        """This method is used to deploy the asset in target enviroment"""

        schemas = set(self.schemas.keys())

        if select_object.isdefined():
            if len(select_object.schemas) > 0:
                selected_schemas = set(select_object.schemas)
                identified_schemas = selected_schemas.intersection(schemas)
                schemas = identified_schemas
            else:
                schemas = set()

        deployed_schemas = []

        for schema in schemas:
            deployed_schemas.append(self.schemas[schema].deploy_asset(select_object))
        return deployed_schemas


class TaskContractObject(BaseContractObject):
    """This is task contract object"""

    def __init__(
        self,
        task_location: str,
        project_location: str,
        env: ContractElementEnvironmentSchema,
        environment: EnvTypes = EnvTypes.local,
    ):
        task_name = os.path.basename(task_location).replace(".json", "")
        self.name = task_name
        self.location = task_location
        self.project_location = project_location
        self.validation_error = []
        try:
            task_json_config = read_json_file(task_location)
            self.task_config = TaskObjectSchema(**task_json_config)
            task_config = self.format_task_variables(
                self.task_config.task,
                getattr(getattr(self.task_config.env, environment.value), "variables"),
                env,
                environment,
            )
            task_config = TaskObjectSchema.cast(task_config)
            self.config = task_config

        except Exception as e:
            self.validation_error.append(
                Check(
                    name="check_task_config_schema",
                    object_type="task",
                    object_name=task_name,
                    description="This check validates whether task json configuration is valid or not",
                    details=f"Error in parsing the configuration with error {e}",
                    result=ResultEnum.error,
                    location=task_location,
                )
            )

    def format_task_variables(
        self, source_object, local_env, global_environ, environment
    ):
        """This method is used for formatting the pipeline global varibales"""
        global_env = global_environ.global_variables
        if local_env is None:
            local_env = {}
        if global_env is None:
            global_env = {}
        global_global_env = Environment().get_env()
        local_environ = {**local_env, **global_env}
        env = {**local_environ, **global_global_env}

        datasets_pattern = DATASET_PARSE_PATTERN
        external_file_pattern = EXTERNAL_PARSE_PATTERN
        local_env_schema = ContractElementEnvironmentSchema(
            global_variables=local_environ, presetting=global_environ.presetting
        )

        def string_format_variables(str_object):
            datasets_match = re.match(datasets_pattern, str_object)
            if datasets_match is not None:
                str_object = self.resolve_table_reference(
                    datasets_pattern,
                    str_object,
                    local_env_schema,
                    environment,
                )
                if isinstance(str_object, dict):
                    str_object = self.format_task_variables(
                        str_object,
                        local_env=local_env,
                        global_environ=global_environ,
                        environment=environment,
                    )
                    return str_object
            external_file_path = re.match(external_file_pattern, str_object)
            if external_file_path is not None:
                str_object = self.resolve_external_file_reference(
                    external_file_pattern,
                    str_object,
                    local_env_schema,
                    environment,
                )
                if isinstance(str_object, dict):
                    str_object = self.format_task_variables(
                        str_object,
                        local_env=local_env,
                        global_environ=global_environ,
                        environment=environment,
                    )
                    return str_object
            str_object = replace_environment_variables(str_object, env)
            return str_object

        def list_formatting_variables(list_object):
            local_result = []
            for ele in list_object:
                if isinstance(ele, dict):
                    local_result.append(
                        self.format_task_variables(
                            ele,
                            local_env=local_env,
                            global_environ=global_environ,
                            environment=environment,
                        )
                    )
                elif isinstance(ele, str):
                    ele = string_format_variables(ele)
                    local_result.append(ele)
                elif isinstance(ele, list):
                    local_result.extend(list_formatting_variables(ele))
            return local_result

        for option in source_object.keys():
            key_option = option
            if isinstance(source_object[key_option], str):
                source_object[key_option] = string_format_variables(
                    source_object[key_option]
                )
            elif isinstance(source_object[key_option], dict):
                source_object[key_option] = self.format_task_variables(
                    source_object[key_option],
                    local_env=local_env,
                    global_environ=global_environ,
                    environment=environment,
                )
            elif isinstance(source_object[key_option], list):
                source_object[key_option] = list_formatting_variables(
                    source_object[key_option]
                )
        return source_object

    def resolve_table_reference(
        self,
        pattern,
        table_reference,
        env,
        environment: EnvTypes = EnvTypes.local,
    ):
        """This method is used to resolve the dataset table reference in pipeline configuration"""
        table_match = re.match(pattern, table_reference)
        if table_match is None:
            return table_reference
        root_folder = table_match.group(1).replace(".", os.path.sep)
        schema_name = table_match.group(2)
        table_name = table_match.group(3)

        table_path = os.path.join(
            pathlib.Path(self.project_location).resolve(),
            root_folder,
            schema_name,
            "tables",
            f"{table_name}.json",
        )

        if not os.path.exists(table_path):
            self.validation_error.append(
                Check(
                    name="check_task_config_schema",
                    object_type="task",
                    object_name=self.name,
                    description="This check validates whether task json configuration is valid or not",
                    details=f"Error in parsing the configuration where in task configuration there is a datasets table reference {table_reference} which is not able to resolve",
                    result=ResultEnum.error,
                    location=self.location,
                )
            )
            return table_reference

        table = TableContractObject(
            table_location=table_path, env=env, environment=environment
        )

        if table.validation_error is None:
            if table.config.schema_name is None:
                schema_name = (
                    schema_name + "_" + env.presetting.name_suffix
                    if (
                        env.presetting.name_suffix is not None
                        and len(env.presetting.name_suffix) > 0
                    )
                    else schema_name
                )
                if Environment().OF_MODE != EnvironmentMode.run:
                    schema_name = (
                        env.presetting.name_prefix + "_" + schema_name
                        if (
                            env.presetting.name_prefix is not None
                            and len(env.presetting.name_prefix) > 0
                        )
                        else schema_name
                    )
                table.config.schema_name = schema_name
            return table.config.to_json()
        else:
            self.validation_error.append(
                Check(
                    name="check_pipeline_config_schema",
                    object_type="pipeline",
                    object_name=self.name,
                    description="This check validates whether pipeline json configuration is valid or not",
                    details=f"Error in parsing the configuration where in pipeline configuration there is a problem in resolving the datasets table reference {table_reference} and failed due to {table.validation_error.details}",
                    result=ResultEnum.error,
                    location=self.location,
                )
            )
            return table_reference

    def resolve_external_file_reference(
        self,
        pattern,
        file_reference,
        env,
        environment: EnvTypes = EnvTypes.local,
    ):
        """This method is used to resolve the dataset external reference in pipeline configuration"""
        file_match = re.match(pattern, file_reference)
        if file_match is None:
            return file_reference
        root_folder = file_match.group(1).replace(".", os.path.sep)
        schema_name = file_match.group(2)
        file_name = file_match.group(3)

        external_files_dir = os.path.join(
            pathlib.Path(self.project_location).resolve(),
            root_folder,
            schema_name,
            EXTERNAL_FILES_VOLUME_NAME,
        )

        schema_name = (
            schema_name + "_" + env.presetting.name_suffix
            if (
                env.presetting.name_suffix is not None
                and len(env.presetting.name_suffix) > 0
            )
            else schema_name
        )
        if Environment().OF_MODE != EnvironmentMode.run:
            schema_name = (
                env.presetting.name_prefix + "_" + schema_name
                if (
                    env.presetting.name_prefix is not None
                    and len(env.presetting.name_prefix) > 0
                )
                else schema_name
            )

        external_files_list = os.listdir(external_files_dir)

        for file in external_files_list:
            external_path = os.path.join(external_files_dir, file)
            if os.path.isfile(external_path) and file.split(".")[0] == file_name:
                ref_file = f"#ref(external,volume,{schema_name},{external_path},{file},{file.split('.')[0]})"
                return ref_file
        self.validation_error.append(
            Check(
                name="check_task_config_schema",
                object_type="task",
                object_name=self.name,
                description="This check validates whether task json configuration is valid or not",
                details=f"Error in parsing the configuration where in task configuration there is a external file reference {file_reference} which is not able to resolve",
                result=ResultEnum.error,
                location=self.location,
            )
        )
        return file_reference

    def validate(self, run: Run) -> None:
        """Not Implemented"""

    def deploy_asset(self, select_object=None):
        """Not Implemented"""


class PipelineContractObject(BaseContractObject):
    """This class is the class definition for the pipeline contract object"""

    is_modifiable_by_env = True

    def __init__(
        self,
        project_name: str,
        project_location: str,
        pipeline_config_location: str,
        env: ContractElementEnvironmentSchema,
        environment: EnvTypes = EnvTypes.local,
    ):
        global_environment = Environment()
        pipeline_name = os.path.basename(pipeline_config_location).replace(".json", "")
        pipeline_location = pathlib.Path(pipeline_config_location).resolve().parent
        project_root_name = os.path.basename(project_location)
        project_location_path = pathlib.Path(project_location).resolve().parent
        self.project_location = project_location
        self.pipeline_module = (
            pipeline_location.relative_to(project_location_path)
            .__str__()
            .replace(os.path.sep, ".")
            .replace(project_root_name, project_name)
        )
        self.name = pipeline_name
        self.location = pipeline_location.__str__()
        self.config_location = pipeline_config_location
        self.project_name = project_name
        self.validation_error = []

        self.docs = os.path.join(pipeline_location.__str__(), "docs")
        self.execution = os.path.join(pipeline_location.__str__(), "execution")
        try:

            if not os.path.exists(self.docs):
                self.validation_error.append(
                    Check(
                        name="check_pipeline_docs",
                        object_type="pipeline",
                        object_name=pipeline_name,
                        description="This check validates whether pipeline docs folder exists or not",
                        details="Pipeline docs folder doesn't exist. Please upload the required docs folder to ignore the validation issue",
                        result=ResultEnum.warning,
                        location=pipeline_config_location,
                    )
                )
            pipeline_json = read_json_file(pipeline_config_location)
            self.pipeline_config = PipelineObjectSchema(**pipeline_json)
            pipeline_config = self.format_pipeline_variables(
                self.pipeline_config.pipeline,
                getattr(
                    getattr(self.pipeline_config.env, environment.value),
                    "variables",
                ),
                env,
                environment,
            )
            pipeline_config = PipelineObjectSchema.cast(pipeline_config)
            self.config = pipeline_config
            pipeline_name = self.config.name

            pipeline_name = (
                pipeline_name + "_" + env.presetting.name_suffix
                if (
                    env.presetting.name_suffix is not None
                    and len(env.presetting.name_suffix) > 0
                )
                else pipeline_name
            )

            pipeline_name = (
                env.presetting.name_prefix + "_" + pipeline_name
                if (
                    env.presetting.name_prefix is not None
                    and len(env.presetting.name_prefix) > 0
                )
                else pipeline_name
            )
            self.config.name = pipeline_name

            tags = self.config.tags.copy() if self.config.tags is not None else {}
            self.config.tags = {
                **env.presetting.tags,
                **global_environment.OF_PIPELINE_TAGS,
            }
            self.config.tags = {**self.config.tags, **tags}

            if not os.path.exists(self.execution):

                if not all(
                    [
                        self.config.tasks[task].type
                        == PipelineTaskTypes.spark_pipeline_task
                        for task in self.config.tasks
                    ]
                ):

                    self.validation_error.append(
                        Check(
                            name="check_pipeline_execution",
                            object_type="pipeline",
                            object_name=pipeline_name,
                            description="This check validates whether pipeline execution folder where dags are stored doesn't exists or not",
                            result=ResultEnum.error,
                            location=pipeline_config_location,
                        )
                    )
                else:
                    self.execution = None

        except Exception as e:
            self.validation_error.append(
                Check(
                    name="check_pipeline_config_schema",
                    object_type="pipeline",
                    object_name=pipeline_name,
                    description="This check validates whether pipeline json configuration is valid or not",
                    details=f"Error in parsing the configuration with error {e}",
                    result=ResultEnum.error,
                    location=pipeline_config_location,
                )
            )

    def format_pipeline_variables(
        self, source_object, local_env, global_environ, environment
    ):
        """This method is used for formatting the pipeline global varibales"""
        global_env = global_environ.global_variables
        if local_env is None:
            local_env = {}
        if global_env is None:
            global_env = {}
        global_global_env = Environment().get_env()
        local_environ = {**local_env, **global_env}
        env = {**local_environ, **global_global_env}

        datasets_pattern = DATASET_PARSE_PATTERN
        task_pattern = TASK_PARSE_PATTERN
        external_file_pattern = EXTERNAL_PARSE_PATTERN
        local_env_schema = ContractElementEnvironmentSchema(
            global_variables=local_environ, presetting=global_environ.presetting
        )

        def string_format_variables(str_object):
            datasets_match = re.match(datasets_pattern, str_object)
            if datasets_match is not None:
                str_object = self.resolve_table_reference(
                    datasets_pattern,
                    str_object,
                    local_env_schema,
                    environment,
                )
                if isinstance(str_object, dict):
                    str_object = self.format_pipeline_variables(
                        str_object,
                        local_env=local_env,
                        global_environ=global_environ,
                        environment=environment,
                    )
                    return str_object
            tasks_match = re.match(task_pattern, str_object)
            if tasks_match is not None:
                str_object = self.resolve_task_reference(
                    task_pattern,
                    str_object,
                    local_env_schema,
                    environment,
                )
                if isinstance(str_object, dict):
                    str_object = self.format_pipeline_variables(
                        str_object,
                        local_env=local_env,
                        global_environ=global_environ,
                        environment=environment,
                    )
                    return str_object

            external_file_path = re.match(external_file_pattern, str_object)
            if external_file_path is not None:
                str_object = self.resolve_external_file_reference(
                    external_file_pattern,
                    str_object,
                    local_env_schema,
                    environment,
                )
                if isinstance(str_object, dict):
                    str_object = self.format_pipeline_variables(
                        str_object,
                        local_env=local_env,
                        global_environ=global_environ,
                        environment=environment,
                    )
                    return str_object
            str_object = replace_environment_variables(str_object, env)
            return str_object

        def list_formatting_variables(list_object):
            local_result = []
            for ele in list_object:
                if isinstance(ele, dict):
                    local_result.append(
                        self.format_pipeline_variables(
                            ele,
                            local_env=local_env,
                            global_environ=global_environ,
                            environment=environment,
                        )
                    )
                elif isinstance(ele, str):
                    ele = string_format_variables(ele)
                    local_result.append(ele)
                elif isinstance(ele, list):
                    local_result.extend(list_formatting_variables(ele))
            return local_result

        for option in source_object.keys():
            key_option = option
            if isinstance(source_object[key_option], str):
                source_object[key_option] = string_format_variables(
                    source_object[key_option]
                )
            elif isinstance(source_object[key_option], dict):
                source_object[key_option] = self.format_pipeline_variables(
                    source_object[key_option],
                    local_env=local_env,
                    global_environ=global_environ,
                    environment=environment,
                )
            elif isinstance(source_object[key_option], list):
                source_object[key_option] = list_formatting_variables(
                    source_object[key_option]
                )
        return source_object

    def resolve_table_reference(
        self,
        pattern,
        table_reference,
        env,
        environment: EnvTypes = EnvTypes.local,
    ):
        """This method is used to resolve the dataset table reference in pipeline configuration"""
        table_match = re.match(pattern, table_reference)
        if table_match is None:
            return table_reference
        root_folder = table_match.group(1).replace(".", os.path.sep)
        schema_name = table_match.group(2)
        table_name = table_match.group(3)

        table_path = os.path.join(
            pathlib.Path(self.project_location).resolve(),
            root_folder,
            schema_name,
            "tables",
            f"{table_name}.json",
        )

        if not os.path.exists(table_path):
            self.validation_error.append(
                Check(
                    name="check_pipeline_config_schema",
                    object_type="pipeline",
                    object_name=self.name,
                    description="This check validates whether pipeline json configuration is valid or not",
                    details=f"Error in parsing the configuration where in pipeline configuration there is a datasets table reference {table_reference} which is not able to resolve",
                    result=ResultEnum.error,
                    location=self.config_location,
                )
            )
            return table_reference

        table = TableContractObject(
            table_location=table_path, env=env, environment=environment
        )

        if table.validation_error is None:
            if table.config.schema_name is None:
                schema_name = (
                    schema_name + "_" + env.presetting.name_suffix
                    if (
                        env.presetting.name_suffix is not None
                        and len(env.presetting.name_suffix) > 0
                    )
                    else schema_name
                )
                if Environment().OF_MODE != EnvironmentMode.run:
                    schema_name = (
                        env.presetting.name_prefix + "_" + schema_name
                        if (
                            env.presetting.name_prefix is not None
                            and len(env.presetting.name_prefix) > 0
                        )
                        else schema_name
                    )
                table.config.schema_name = schema_name
            return table.config.to_json()
        else:
            self.validation_error.append(
                Check(
                    name="check_pipeline_config_schema",
                    object_type="pipeline",
                    object_name=self.name,
                    description="This check validates whether pipeline json configuration is valid or not",
                    details=f"Error in parsing the configuration where in pipeline configuration there is a problem in resolving the datasets table reference {table_reference} and failed due to {table.validation_error.details}",
                    result=ResultEnum.error,
                    location=self.config_location,
                )
            )
            return table_reference

    def resolve_external_file_reference(
        self,
        pattern,
        file_reference,
        env,
        environment: EnvTypes = EnvTypes.local,
    ):
        """This method is used to resolve the dataset external reference in pipeline configuration"""
        file_match = re.match(pattern, file_reference)
        if file_match is None:
            return file_reference
        root_folder = file_match.group(1).replace(".", os.path.sep)
        schema_name = file_match.group(2)
        file_name = file_match.group(3)

        external_files_dir = os.path.join(
            pathlib.Path(self.project_location).resolve(),
            root_folder,
            schema_name,
            EXTERNAL_FILES_VOLUME_NAME,
        )

        schema_name = (
            schema_name + "_" + env.presetting.name_suffix
            if (
                env.presetting.name_suffix is not None
                and len(env.presetting.name_suffix) > 0
            )
            else schema_name
        )
        if Environment().OF_MODE != EnvironmentMode.run:
            schema_name = (
                env.presetting.name_prefix + "_" + schema_name
                if (
                    env.presetting.name_prefix is not None
                    and len(env.presetting.name_prefix) > 0
                )
                else schema_name
            )

        external_files_list = os.listdir(external_files_dir)

        for file in external_files_list:
            external_path = os.path.join(external_files_dir, file)
            if os.path.isfile(external_path) and file.split(".")[0] == file_name:
                ref_file = f"#ref(external,volume,{schema_name},{external_path},{file},{file.split('.')[0]})"
                return ref_file
        self.validation_error.append(
            Check(
                name="check_task_config_schema",
                object_type="task",
                object_name=self.name,
                description="This check validates whether task json configuration is valid or not",
                details=f"Error in parsing the configuration where in task configuration there is a external file reference {file_reference} which is not able to resolve",
                result=ResultEnum.error,
                location=self.location,
            )
        )
        return file_reference

    def resolve_task_reference(
        self,
        pattern,
        task_reference,
        env,
        environment: EnvTypes = EnvTypes.local,
    ):
        """This method is used to resolve the task reference in pipeline configuration"""

        task_match = re.match(pattern, task_reference)
        if task_match is None:
            return task_reference
        task_file = task_match.group(1)

        task_path = os.path.join(
            self.location, "tasks", f"{task_file.replace('.',os.path.sep)}.json"
        )

        if not os.path.exists(task_path):
            self.validation_error.append(
                Check(
                    name="check_pipeline_config_schema",
                    object_type="pipeline",
                    object_name=self.name,
                    description="This check validates whether pipeline json configuration is valid or not",
                    details=f"Error in parsing the configuration where in pipeline tasks there is a reference {task_reference} which is not able to resolve, where corresponding task file doesn't exist",
                    result=ResultEnum.error,
                    location=self.config_location,
                )
            )
            return task_reference

        task = TaskContractObject(
            task_location=task_path,
            env=env,
            environment=environment,
            project_location=self.project_location,
        )
        if task.validation_error is None or (
            isinstance(task.validation_error, list) and len(task.validation_error) == 0
        ):
            result = task.config.to_json()
            return result
        else:
            if (
                isinstance(task.validation_error, list)
                and len(task.validation_error) > 0
            ):
                self.validation_error.extend(task.validation_error)
            self.validation_error.append(
                Check(
                    name="check_pipeline_config_schema",
                    object_type="pipeline",
                    object_name=self.name,
                    description="This check validates whether pipeline json configuration is valid or not",
                    details=f"Error in parsing the task configuration, there is problem in resolving the task reference {task_reference} and failed check there validation result for detailed output",
                    result=ResultEnum.error,
                    location=self.config_location,
                )
            )
            return task_reference

    def validate(self, run: Run, select_object: SelectObject = SelectObject()) -> None:
        """This is the validate method to run the validation on the pipeline object"""
        pipelines = (
            select_object.pipelines
            if (select_object is not None) and select_object.isdefined()
            else []
        )
        if (self.validation_error is None) or (
            all(
                [
                    validation.result not in [ResultEnum.error, ResultEnum.failed]
                    for validation in self.validation_error
                ]
            )
        ):
            PipelineValidation.validate(
                run=run,
                pipeline=self.config,
                name=self.name,
                docs=self.docs,
                pipeline_module=self.pipeline_module,
                object_location=self.location,
                pipelines=pipelines,
            )
        if self.validation_error is not None:
            run.extend(self.validation_error)
        return

    @property
    def type(self):
        """This property returns the type of the pipeline"""
        return self.config.type

    def deploy_asset(
        self, select_object: SelectObject = SelectObject()
    ) -> DeployConfig:
        """This method is used to deploy the asset in target environment"""
        configuration = self.config.model_copy(deep=True)
        if configuration.type == PipelineTypes.spark:
            for task in configuration.tasks:
                if configuration.tasks[task].type == PipelineTaskTypes.spark_task:
                    configuration.tasks[task].execution.source = (
                        f"project.{self.pipeline_module}.{configuration.tasks[task].execution.source}"
                    )

            pipeline_deploy_obj = cast(SparkPipelineDeloyConfig, configuration)
            return pipeline_deploy_obj


class PipelinesContractObject(BaseContractObject):
    """This is the class to hold the schema and table object"""

    is_modifiable_by_env = True

    def __init__(
        self,
        project_name: str,
        project_location: str,
        pipeline_location: list,
        env: ContractElementEnvironmentSchema,
        environment: EnvTypes = EnvTypes.local,
    ):
        self.validation_error = []
        self.pipelines = SimpleNamespace()
        self.project_name = project_name

        for folder in pipeline_location:
            folder = os.path.join(project_location, folder)
            if not os.path.exists(folder):
                raise ProjectContractPipelineNotExists(
                    f"Project contract has defined template in respect to pipeline, where pipeline folder  which was provided {folder} doesn't exist, Please check the project configuration file"
                )
            pipelines = os.listdir(folder)

            if len(pipelines) == 0:
                self.validation_error.append(
                    Check(
                        name="check_pipelines_folder",
                        object_type="pipeline",
                        description="This check validates whether pipelines folders exists or not",
                        details="No pipelines folder exists",
                        result=ResultEnum.error,
                        location=folder,
                    )
                )
                continue
            for pipeline in pipelines:
                pipeline_location = os.path.join(folder, pipeline)

                pipeline_config_location = os.path.join(
                    pipeline_location, f"{pipeline}.json"
                )

                if not os.path.exists(pipeline_config_location):
                    self.validation_error.append(
                        Check(
                            name="check_pipeline_file_exists",
                            object_type="pipeline",
                            object_name=pipeline,
                            description="This checks whether schema json configuration file exists or not",
                            details=f"No schema file with name {pipeline}.json exists",
                            result=ResultEnum.error,
                            location=pipeline_config_location,
                        )
                    )
                    continue

                pipelines_required_folders = os.listdir(pipeline_location)

                if (
                    len(
                        REQUIRED_PIPELINE_FOLDERS.difference(
                            set(pipelines_required_folders)
                        )
                    )
                    > 0
                ):
                    self.validation_error.append(
                        Check(
                            name="check_pipeline_required_folders",
                            object_type="pipeline",
                            object_name=pipeline,
                            description="This checks whether required folders exists or not",
                            details=f"missing required folders {REQUIRED_PIPELINE_FOLDERS.difference(set(pipelines_required_folders))}",
                            result=ResultEnum.error,
                            location=pipeline_config_location,
                        )
                    )
                    continue

                pipeline_obj = PipelineContractObject(
                    project_name=project_name,
                    project_location=project_location,
                    pipeline_config_location=pipeline_config_location,
                    env=env,
                    environment=environment,
                )
                self.pipelines[pipeline_obj.name] = pipeline_obj

    def validate(self, run: Run, select_object: SelectObject = SelectObject()) -> None:
        """This method is used for validation of the project object"""

        pipelines = set(self.pipelines.keys())

        if select_object.isdefined():
            if len(select_object.pipelines) > 0:
                selected_pipelines = set(select_object.pipelines)
                identified_pipelines = selected_pipelines.difference(pipelines)

                if len(identified_pipelines) > 0:
                    pipelines = set()

                    self.validation_error.append(
                        Check(
                            name="check_selected_pipeline",
                            object_type="pipeline",
                            object_name=",".join(list(identified_pipelines)),
                            description="This checks whether selected pipeline exists or not",
                            details=f"missing provided pipeline {','.join(list(identified_pipelines))}",
                            result=ResultEnum.error,
                            location=None,
                        )
                    )
                else:
                    pipelines = selected_pipelines
            else:
                pipelines = set()
        pipeline_select_object = SelectObject(
            pipelines=[
                f"{self.pipelines[pipeline].config.name}||{self.pipelines[pipeline].name}"
                for pipeline in self.pipelines.keys()
                if (self.pipelines[pipeline].validation_error is None)
                or (
                    all(
                        [
                            validation.result
                            not in [ResultEnum.error, ResultEnum.failed]
                            for validation in self.pipelines[pipeline].validation_error
                        ]
                    )
                )
            ]
        )
        for pipeline in pipelines:
            self.pipelines[pipeline].validate(run, pipeline_select_object)
        if self.validation_error is not None:
            run.extend(self.validation_error)
        return

    def deploy_asset(
        self, select_object: SelectObject = SelectObject()
    ) -> List[DeployConfig]:
        """This method is used to deploy the asset in target enviroment"""

        pipelines = set(self.pipelines.keys())

        if select_object.isdefined():
            if len(select_object.pipelines) > 0:
                selected_pipelines = set(select_object.pipelines)
                identified_pipelines = selected_pipelines.intersection(pipelines)
                pipelines = identified_pipelines
            else:
                pipelines = set()

        deployed_pipelines = {PipelineTypes.spark: {}}
        for pipeline in pipelines:
            deployed_pipelines[self.pipelines[pipeline].type][pipeline] = (
                self.pipelines[pipeline].deploy_asset()
            )
        return deployed_pipelines
