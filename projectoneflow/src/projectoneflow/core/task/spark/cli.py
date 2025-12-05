"""This script is cli implementation of the spark task execution"""

import argparse
import logging
import json
import colorlog.escape_codes
from projectoneflow.core.schemas.deploy import SparkTask as SparkTaskSchema
from projectoneflow.core.cli import CliGroup
from projectoneflow.core.task.spark import SparkTask
from projectoneflow.core.observability import Logger
import sys
from pydantic import ValidationError
import colorlog
import os


class SparkTaskCliGroup(CliGroup):
    def __init__(self, spark_parser: argparse.ArgumentParser):
        """
        This is initialization method for task cli group

        Parameters
        ----------------
        parser: argparse.ArgumentParser
            parent parser to which child parser will be initialized
        """
        self.parser = spark_parser
        self.sub_parser = spark_parser.add_subparsers(
            title="Spark Task Commands", dest="spark_command"
        )

        self.__initialize_spark_command()

    def __initialize_spark_command(self):
        """This method initializes the spark command"""

        run_parser = self.sub_parser.add_parser(
            prog="projectoneflow task spark run",
            name="run",
            help="Execute the spark projectoneflow task in local/databricks environment",
        )
        run_parser.add_argument(
            "-d",
            "--debug",
            action="store_true",
            help="when specified execute in debug mode, where logging will be more intuitive",
        )
        run_parser.add_argument(
            "-t",
            "--task_config",
            required=True,
            type=str,
            help="Json String/file to be passed to configure spark task and execute",
        )
        validate_parser = self.sub_parser.add_parser(
            prog="projectoneflow task spark validate",
            name="validate",
            help="Checks/Validates the spark projectoneflow task configuration",
        )
        validate_parser.add_argument(
            "-t",
            "--task_config",
            required=True,
            type=str,
            help="Json String/file to be passed to configure spark task and execute",
        )

    def create_spark_task_config_object(self, task_config: str):
        """
        This method is used to create the spark task object
        """
        try:
            if task_config.endswith(".json"):
                if os.path.exists(task_config):
                    with open(task_config, "r") as f:
                        task_config = f.read()
            json_config = json.loads(task_config)
            return (SparkTaskSchema(**json_config), None, None)
        except json.JSONDecodeError as e:
            return (None, e, json.JSONDecodeError)
        except ValidationError as e:
            return (None, e, ValidationError)
        except Exception as e:
            return (None, e, e.__class__)

    def validate(self, task_config: str):
        """
        Validates the task configuration which is passed as json string

        Parameters
        ----------------------
        task_config:str
            spark task configuration to be parsed and validated
        """
        (
            spark_task_config_obj,
            exception,
            exception_type,
        ) = self.create_spark_task_config_object(task_config)
        if exception:
            if exception_type == json.JSONDecodeError:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Failed: \n \t Task configuration JSON Parsing Error: {exception}\n{colorlog.escape_codes.escape_codes['reset']}"
                )
                sys.exit(-1)
            elif exception_type == ValidationError:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Failed: \n \t Task configuration validation Errors: {exception}\n{colorlog.escape_codes.escape_codes['reset']}"
                )
                sys.exit(-1)
            else:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Failed: \n \t Task configuration validation Errors: {exception} \n{colorlog.escape_codes.escape_codes['reset']}"
                )
                sys.exit(-1)

        else:
            sys.stdout.write(
                f"{colorlog.escape_codes.escape_codes['bold_green']}Validation Succeeded!!! No Issues Found... \n{colorlog.escape_codes.escape_codes['reset']}"
            )
            sys.exit(0)

    def run(self, task_config: str, debug: bool = True):
        """
        Executes the task where passed with passed configuration

        Parameters
        ----------------
        task_config:str
            spark task configuration to be parsed and executed
        debug:bool
            whether to execute the projectoneflow task execution in debug mode
        """

        if debug:
            Logger.set_level(logging.DEBUG)
        else:
            Logger.set_level(logging.INFO)

        (
            spark_task_config_obj,
            exception,
            exception_type,
        ) = self.create_spark_task_config_object(task_config=task_config)
        if exception:
            if exception_type == json.JSONDecodeError:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Error: \n \t Task configuration JSON Parsing Error: {exception}\n{colorlog.escape_codes.escape_codes['reset']}"
                )
                sys.exit(-1)
            elif exception_type == ValidationError:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Error: \n \t Task configuration validation Errors: {exception}\n{colorlog.escape_codes.escape_codes['reset']}"
                )
                sys.exit(-1)
            else:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Validation Failed: \n \t Task configuration validation Errors: {exception}\n{colorlog.escape_codes.escape_codes['reset']}"
                )
                sys.exit(-1)

        spark_obj = (
            SparkTask.builder.setName(spark_task_config_obj.name)
            .setInput(spark_task_config_obj.input)
            .setOutput(spark_task_config_obj.output)
            .setExecution(spark_task_config_obj.execution)
        )

        if spark_task_config_obj.refresh_policy is not None:
            spark_obj.setRefreshPolicy(spark_task_config_obj.refresh_policy)
        if spark_task_config_obj.extra_spark_configuration is not None:
            spark_obj.setSparkconfigs(spark_task_config_obj.extra_spark_configuration)
        if spark_task_config_obj.metadata_location_path is not None:
            spark_obj.setMetadataLog(spark_task_config_obj.metadata_location_path)
        if spark_task_config_obj.secret_file_path is not None:
            spark_obj.setSecret(secret_file=spark_task_config_obj.secret_file_path)

        try:
            spark_task_obj = spark_obj.create()
        except Exception as e:
            sys.stderr.write(
                f"{colorlog.escape_codes.escape_codes['bold_red']}Spark Task Creation Failed: \n \t {e}\n{colorlog.escape_codes.escape_codes['reset']}"
            )
            sys.exit(-1)

        try:
            spark_task_obj.execute()
        except Exception as e:
            sys.stderr.write(
                f"{colorlog.escape_codes.escape_codes['bold_red']}Spark Task Execution Failed: \n \t {e}\n{colorlog.escape_codes.escape_codes['reset']}"
            )
            sys.exit(-1)

    def execute(self, args):
        """
        This is the method which parses and executes the spark task method
        """
        if args.spark_command is None:
            self.parser.print_help()
        else:
            if args.spark_command == "run":
                self.run(args.task_config, args.debug)

            elif args.spark_command == "validate":
                self.validate(args.task_config)
