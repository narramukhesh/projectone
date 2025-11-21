import os
from oneflow.framework.contract.env import (
    EnvTypes,
    format_environment_variables,
)
from oneflow.framework.validation import Run, Check, ResultEnum

from oneflow.framework.exception.contract import (
    ProjectContractNotExists,
    ProjectConfigValidationError,
    ProjectArtifactCreationError,
    DeployStrategyDoesnotExist,
)
from oneflow.framework.contract.config import (
    ProjectContractSchema,
    SelectObject,
    DeploySchema,
)
from oneflow.framework.contract.config.objects import (
    DatasetContractObject,
    PipelinesContractObject,
)
from oneflow.core.utils import read_json_file
from oneflow.framework.contract import Contract
import subprocess
from oneflow.framework.utils import is_windows
from oneflow.framework.contract.env import Environment
from oneflow.framework.contract.strategy import DeployStrategyTypes


class ProjectContract(Contract):
    """This class is implementation of the project specification contract"""

    def __init__(self, folder_location: str, environment: EnvTypes = EnvTypes.local):
        self.__initialize__(folder_location, environment)
        self.project_location = os.path.abspath(folder_location)
        self.environment = environment
        self.transform_deploy_folder = None

    def __initialize__(self, root_folder: str, environment: EnvTypes = EnvTypes.local):
        """This methods initializes the project contract"""
        project_location = os.path.abspath(root_folder)

        if not os.path.exists(project_location):
            raise ProjectContractNotExists(
                f"Provided folder {project_location} doesn't exist"
            )

        self.project_name = os.path.basename(project_location)

        project_config_location = os.path.join(
            project_location, f"{self.project_name}.json"
        )

        if not os.path.exists(project_config_location):
            raise ProjectContractNotExists(
                f"Project contract configuration {project_config_location} file doesn't exist, Please check"
            )

        try:
            project_config = ProjectContractSchema(
                **read_json_file(project_config_location)
            )
            if project_config.name is not None:
                self.project_name = project_config.name
        except Exception as e:
            raise ProjectConfigValidationError(
                f"Validation error while parsing the project Contract file {project_config_location} which was failed with error {e}"
            )

        self.config = project_config
        self.config.dataset = DatasetContractObject(
            self.project_name,
            project_location,
            project_config.dataset,
            # getattr(getattr(project_config.env, environment.value), "dataset"),
            getattr(project_config.env, environment.value),
            environment=environment,
        )

        self.config.pipelines = PipelinesContractObject(
            self.project_name,
            project_location,
            project_config.pipelines,
            # getattr(getattr(project_config.env, environment.value), "pipelines"),
            getattr(project_config.env, environment.value),
            environment=environment,
        )

        deploy = format_environment_variables(
            source_object=self.config.deploy.to_json(),
            local_env=None,
            global_env=getattr(
                getattr(project_config.env, environment.value),
                "global_variables",
            ),
        )
        self.config.deploy = DeploySchema(**deploy)
        self.config.transform = project_config.transform

    def setup_transform_folder(self, package_folder: list):
        """This method will setup the package directory for the project folder"""
        import tempfile
        import os
        import shutil
        import sys
        import pathlib

        project_parent_location = pathlib.Path(self.project_location).resolve().parent

        deploy_directory = os.path.join(
            tempfile.gettempdir(), ".oneflow", "deploy", self.environment.value
        )
        project_deploy_directory = os.path.join(deploy_directory, "project")
        if not os.path.exists(project_deploy_directory):
            os.makedirs(project_deploy_directory, exist_ok=True)
        for i in package_folder:
            dest_location = pathlib.Path(i)
            difference = dest_location.relative_to(project_parent_location).__str__()
            dest_folder = os.path.join(project_deploy_directory, difference)
            if not os.path.exists(dest_folder):
                os.makedirs(dest_folder, exist_ok=True)
            shutil.copytree(i, dest_folder, dirs_exist_ok=True)
        sys.path.append(deploy_directory)
        self.transform_deploy_folder = deploy_directory

    def get_packaged_artifact(self):
        """
        This method packages the deploy artifacts
        """
        package_folder = (
            self.transform_deploy_folder.replace(os.path.sep, "\\\\")
            if is_windows()
            else self.transform_deploy_folder
        )
        setup_script = f"""
from setuptools import setup, find_packages

setup(
name="project-{self.project_name}",
version="0.1",
packages=find_packages(where="{package_folder}", include=["project.{self.project_name}.*"]),
package_data={{
'': ['*'],  # Include all files in the directory
}},
include_package_data=True,
)
                        """

        project_deploy_directory = os.path.join(self.transform_deploy_folder, "project")
        for root, dirs, files in os.walk(project_deploy_directory):
            if "__init__.py" not in files:
                open(os.path.join(root, "__init__.py"), "w").close()

        with open(os.path.join(self.transform_deploy_folder, "setup.py"), "w") as f:
            f.write(setup_script)

        try:
            subprocess.run(
                ["python", "setup.py", "bdist_wheel"],
                capture_output=True,
                cwd=self.transform_deploy_folder,
                check=True,
            )
        except Exception:
            raise ProjectArtifactCreationError(
                "Problem with artifact creation error, Please check the artifacts like pipeline execution/transform folder if any syntax issues."
            )

        dist_folder = os.path.join(self.transform_deploy_folder, "dist")

        if os.path.exists(dist_folder):
            wheel_files = [f for f in os.listdir(dist_folder) if f.endswith(".whl")]
            if wheel_files:
                latest_wheel = max(
                    wheel_files,
                    key=lambda f: os.path.getctime(os.path.join(dist_folder, f)),
                )
                wheel_path = os.path.join(dist_folder, latest_wheel)
                return wheel_path
            else:
                return None

    def transform_validate(self, run: Run):
        """This method is used to validate the transform folder specified"""
        transform_folder = []
        for transform in self.config.transform:
            folder = os.path.join(self.project_location, transform)
            if not os.path.exists(folder):
                run.append(
                    Check(
                        name="check_project_transform_folders",
                        object_type="project",
                        object_name="transform",
                        description="This checks whether transform folder provided exists or not",
                        details=f"provided transform folder {transform} under {self.project_location} doesn't exists",
                        result=ResultEnum.warning,
                        location=folder,
                    )
                )
                continue
            transform_folder.append(folder)

        for pipeline in self.config.pipelines.pipelines:
            if (
                (self.config.pipelines.pipelines[pipeline].validation_error is None)
                or (
                    all(
                        [
                            validation.result
                            not in [ResultEnum.error, ResultEnum.failed]
                            for validation in self.config.pipelines.pipelines[
                                pipeline
                            ].validation_error
                        ]
                    )
                )
                and (self.config.pipelines.pipelines[pipeline].execution is not None)
            ):
                transform_folder.append(
                    self.config.pipelines.pipelines[pipeline].execution
                )

        self.setup_transform_folder(transform_folder)

    def validate(self, select_object: SelectObject = SelectObject()):
        """
        This method validates the provided folder and nested object structure
        """
        validation_run = Run.create_run()
        self.transform_validate(validation_run)
        self.config.dataset.validate(validation_run, select_object)
        self.config.pipelines.validate(validation_run, select_object)
        validation_run.finish()
        return validation_run

    def get_deploy_strategy(self):
        """
        This method deploys the provided pipeline in target location
        """
        from oneflow.framework.contract.strategy.deploy import TerraformDeployStrategy

        if (self.environment != EnvTypes.local) and (
            Environment().OF_DEPLOY_STRATEGY == DeployStrategyTypes.terraform
        ):
            deploy_strategy = TerraformDeployStrategy(self)
            return deploy_strategy
        raise DeployStrategyDoesnotExist(
            "Selected environment doesn't support the deploy strategy"
        )

    def clear(self):

        if self.transform_deploy_folder is not None:
            import shutil

            shutil.rmtree(self.transform_deploy_folder)
