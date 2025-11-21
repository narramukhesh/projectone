"""This script is cli implementation of the terraform deployment"""

import argparse
import json
import colorlog.escape_codes
from oneflow.core.cli import CliGroup
from oneflow.core.schemas.deploy import (
    SparkPipelineConfig,
    PipelineTaskTypes,
    PipelineTypes,
    DatabricksDeployConfig,
)
import sys
from pydantic import ValidationError
import colorlog
from cdktf import App
import os
import subprocess
import tempfile
from oneflow.core.deploy.terraform import TerraformComponent


class TerraformDeployCliGroup(CliGroup):
    def __init__(self, terraform_parser: argparse.ArgumentParser):
        """
        This is initialization method for terraform task cli group

        Parameters
        ----------------
        parser: argparse.ArgumentParser
            parent parser to which child parser will be initialized
        """
        self.parser = terraform_parser
        self.sub_parser = terraform_parser.add_subparsers(
            title="Terraform Deploy Commands", dest="terraform_command"
        )

        self.__initialize_terraform_command()

    def __initialize_terraform_command(self):
        """This method initializes the spark command"""

        create_parser = self.sub_parser.add_parser(
            prog="oneflow deploy terraform create",
            name="create",
            help="Executes the terraform pipeline deployment task in target spark/databricks environment",
        )
        create_parser.add_argument(
            "-p",
            "--pipeline_config",
            required=True,
            type=str,
            help="Json String to be passed to configure pipeline configuration and execute deployment",
        )
        create_parser.add_argument(
            "-t",
            "--pipeline_type",
            required=True,
            default="spark",
            choices=PipelineTypes.to_list(),
            help="Pipeline type to be used with the pipline config deployment",
        )
        create_parser.add_argument(
            "-o",
            "--target_output_directory",
            required=False,
            type=str,
            help="Directory to where the target terraform scripts are written into target directory",
        )
        create_parser.add_argument(
            "-d",
            "--deploy",
            action="store_true",
            help="Deploy the created terraform scripts at target ",
        )
        create_parser.add_argument(
            "-pl",
            "--plan",
            action="store_true",
            help="Apply terraform deploy plan",
        )
        create_parser.add_argument(
            "-jo" "--json_output",
            required=False,
            type=str,
            help="file path to where the plan output written as json output",
        )
        validate_parser = self.sub_parser.add_parser(
            prog="oneflow deploy terraform validate",
            name="validate",
            help="Checks/Validates the terraform pipeline configuration",
        )
        validate_parser.add_argument(
            "-p",
            "--pipeline_config",
            required=True,
            type=str,
            help="Json String/file to be passed to configure pipeline which is used to deploy in target spark/databricks environment",
        )
        create_parser.add_argument(
            "-t",
            "--pipeline_type",
            required=True,
            default="spark",
            choices=PipelineTypes.to_list(),
            help="Pipeline type to be used with the pipline config deployment",
        )

    def create_pipeline_config_object(self, pipeline_config):
        """
        This method is used to create the spark task object
        """
        try:
            if pipeline_config.endswith(".json"):
                if os.path.exists(pipeline_config):
                    with open(pipeline_config, "r") as f:
                        pipeline_config = f.read()
            json_config = json.loads(pipeline_config)
            return (SparkPipelineConfig(**json_config), None, None)
        except json.JSONDecodeError as e:
            return (None, e, json.JSONDecodeError)
        except ValidationError as e:
            return (None, e, ValidationError)
        except Exception as e:
            return (None, e, e.__class__)

    def validate(self, pipeline_config: str, pipeline_type: str):
        """
        Validates the task configuration which is passed as json string

        Parameters
        ----------------------
        pipeline_config:str
            Pipeline task configuration to be parsed and validated
        pipeline_type:str
            Pipeline task type to be used for selecting the approriate pipeline type
        """
        if pipeline_type != "spark":
            sys.stderr.write(
                f"{colorlog.escape_codes.escape_codes['bold_red']}Sorry!!! Currently pipeline deployment is supported for the spark pipeline type\n{colorlog.escape_codes.escape_codes['reset']}"
            )
            sys.exit(-1)
        (
            pipeline_task_config_obj,
            exception,
            exception_type,
        ) = self.create_pipeline_config_object(pipeline_config)
        if exception:
            if exception_type == json.JSONDecodeError:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Failed: \n \t Pipeline configuration JSON Parsing Error: {exception}\n{colorlog.escape_codes.escape_codes['reset']}"
                )
                sys.exit(-1)
            elif exception_type == ValidationError:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Failed: \n \t Pipeline configuration validation Errors: {exception}\n{colorlog.escape_codes.escape_codes['reset']}"
                )
                sys.exit(-1)
            else:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Failed: \n \t Pipeline configuration validation Errors: {exception} \n{colorlog.escape_codes.escape_codes['reset']}"
                )
                sys.exit(-1)
        else:
            sys.stdout.write(
                f"{colorlog.escape_codes.escape_codes['bold_green']}Validation Succeeded!!! No Issues Found... \n{colorlog.escape_codes.escape_codes['reset']}"
            )
            sys.exit(0)

        try:
            self.create_pipeline_config_object(pipeline_config)
            sys.stdout.write(
                f"{colorlog.escape_codes.escape_codes['bold_green']}Validation Succeeded!!! No Issues Found... \n{colorlog.escape_codes.escape_codes['reset']}"
            )
            sys.exit(0)
        except Exception as e:
            sys.stderr.write(
                f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Failed: \n \t Below are the errors which caused the validation failure {e} \n{colorlog.escape_codes.escape_codes['reset']}"
            )
            sys.exit(-1)

    def create(
        self,
        pipeline_config: str,
        pipeline_type: str = "spark",
        target_directory: str = "./",
        deploy: bool = False,
        plan: str = True,
        json_output: str = None,
    ):
        """
        Executes the pipeline where passed with passed configuration

        Parameters
        ----------------
        pipeline_config:str
            pipeline configuration to be parsed and executed
        target_directory:str
            target directory to where the terraform scripts are saved to
        """

        (
            pipeline_config_obj,
            exception,
            exception_type,
        ) = self.create_pipeline_config_object(
            pipeline_config=pipeline_config, pipeline_type=pipeline_type
        )
        if exception:
            if exception_type == json.JSONDecodeError:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Error: \n \t Pipeline configuration JSON Parsing Error: {exception}\n{colorlog.escape_codes.escape_codes['reset']}"
                )
                sys.exit(-1)
            elif exception_type == ValidationError:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Error: \n \t Pipeline configuration validation Errors: {exception}\n{colorlog.escape_codes.escape_codes['reset']}"
                )
                sys.exit(-1)
            else:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Failed: \n \t Pipeline configuration validation Errors: {exception}\n{colorlog.escape_codes.escape_codes['reset']}"
                )
                sys.exit(-1)

        if all(
            pipeline_config_obj.tasks[task].type == PipelineTaskTypes.spark_task
            for task in pipeline_config_obj.tasks
        ):
            try:
                target_directory = (
                    tempfile.gettempdir()
                    if target_directory is None
                    else target_directory
                )
                current_dir = os.path.abspath(target_directory + "/cdktf.out")
                if os.path.exists(current_dir):
                    os.makedirs(current_dir, mode=777, exist_ok=True)
                terraform_deploy_app = App(outdir=current_dir)
                terraform_component = TerraformComponent(
                    scope=terraform_deploy_app,
                    id=pipeline_config.name,
                )
                databricks_config = DatabricksDeployConfig(
                    pipeline=[pipeline_config_obj],
                )
                terraform_component.add_components("databricks", databricks_config)

                terraform_deploy_app.synth()
                sys.stdout.write(
                    f"""{colorlog.escape_codes.escape_codes['bold_green']}Created the {pipeline_config_obj.name} pipeline terraform configuration at target location {current_dir}. After this please follow steps at each stack directory:\n\t 1. terraform init\n\t 2. terraform plan \n\t 3. terraform apply\n{colorlog.escape_codes.escape_codes['reset']}"""
                )
                if not deploy:
                    sys.stdout.write(
                        f"""{colorlog.escape_codes.escape_codes['bold_green']}After this please follow steps at each stack directory:\n\t 1. terraform init\n\t 2. terraform plan \n\t 3. terraform apply\n{colorlog.escape_codes.escape_codes['reset']}"""
                    )
                if deploy or plan:
                    ti_pid = subprocess.Popen(
                        ["terraform", "init"],
                        stderr=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        cwd=f"{current_dir}/stacks/{pipeline_config_obj.name}",
                    )
                    out, err = ti_pid.communicate()
                    ret_code = ti_pid.returncode
                    out = out.decode()
                    err = err.decode()
                    if ret_code != 0:
                        sys.stderr.write(
                            f"{colorlog.escape_codes.escape_codes['bold_red']}Execution Error: \n \t Pipeline configuration building failed because of the Error: {err}\n{colorlog.escape_codes.escape_codes['reset']}"
                        )
                        sys.exit(-1)

                    ti_plan = subprocess.Popen(
                        ["terraform", "plan", "-input=false"],
                        stderr=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        cwd=f"{current_dir}/stacks/{pipeline_config_obj.name}",
                    )
                    out, err = ti_plan.communicate()
                    ret_code = ti_plan.returncode
                    out = out.decode()
                    err = err.decode()
                    if ret_code != 0:
                        sys.stderr.write(
                            f"{colorlog.escape_codes.escape_codes['bold_red']}Execution Error: \n \t Pipeline configuration building failed because of the Error: {err}\n{colorlog.escape_codes.escape_codes['reset']}"
                        )
                        sys.exit(-1)
                    else:
                        sys.stdout.write(
                            f"""{colorlog.escape_codes.escape_codes['bold_green']}Terraform Plan Output : {out}\n{colorlog.escape_codes.escape_codes['reset']}"""
                        )

                    if deploy:
                        ti_apply = subprocess.Popen(
                            ["terraform", "apply", "-input=false", "-auto-approve"],
                            stderr=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            cwd=f"{current_dir}/stacks/{pipeline_config_obj.name}",
                        )
                        out, err = ti_apply.communicate()
                        ret_code = ti_apply.returncode
                        out = out.decode()
                        err = err.decode()
                        if ret_code != 0:
                            sys.stderr.write(
                                f"{colorlog.escape_codes.escape_codes['bold_red']}Execution Error: \n \t Pipeline configuration building failed because of the Error: {err}\n{colorlog.escape_codes.escape_codes['reset']}"
                            )
                            sys.exit(-1)
                        else:
                            sys.stdout.write(
                                f"""{colorlog.escape_codes.escape_codes['bold_green']}Terraform Plan Output : {out}\n{colorlog.escape_codes.escape_codes['reset']}"""
                            )
                sys.exit(0)
            except Exception as e:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Execution Error: \n \t Pipeline configuration building failed because of the Error: {e}\n{colorlog.escape_codes.escape_codes['reset']}"
                )
                sys.exit(-1)
        else:
            sys.stderr.write(
                f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Error: \n \t Invalid Pipeline task, Please check the task types. It should be one of the below {PipelineTaskTypes.to_list()}\n{colorlog.escape_codes.escape_codes['reset']}"
            )
            sys.exit(-1)

    def execute(self, args):
        """
        This is the method which parses and executes the spark task method
        """
        if args.terraform_command is None:
            self.parser.print_help()
        else:
            if args.terraform_command == "create":
                self.create(
                    pipeline_config=args.pipeline_config,
                    pipeline_type=args.pipeline_type,
                    target_directory=args.target_output_directory,
                )

            elif args.terraform_command == "validate":
                self.validate(
                    pipeline_config=args.pipeline_config,
                    pipeline_type=args.pipeline_type,
                )
