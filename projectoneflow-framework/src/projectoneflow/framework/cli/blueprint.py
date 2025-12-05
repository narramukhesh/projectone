from projectoneflow.core.cli import CliGroup
import argparse
import colorlog
import sys
import json
import os
from typing import List
import colorlog.escape_codes
import importlib
from rich.console import Console
import re
import pathlib

console = Console()


class GenerateTaskCliGroup(CliGroup):
    def __init__(self, generate_parser: argparse.ArgumentParser):
        """
        This is initialization method for contract cli group

        Parameters
        ----------------
        parser: argparse.ArgumentParser
            parent parser to which child parser will be initialized
        """
        self.parser = generate_parser
        self.sub_parser = generate_parser.add_subparsers(
            title="Generate Command", dest="generate_command"
        )

        self.__initialize_contract_command()
        self.__initialize_dataset_command()
        self.__initialize_pipeline_command()

    def __initialize_contract_command(self):
        """This method initializes the contract generation command"""

        contract_parser = self.sub_parser.add_parser(
            prog="oframework blueprint generate contract",
            name="contract",
            help="generates the contract",
        )
        contract_parser.add_argument(
            "-c",
            "--contract_name",
            required=True,
            type=str,
            help="project name where the contract is created",
        )
        contract_parser.add_argument(
            "-f",
            "--contract_folder",
            default="./",
            help="when specified it saves the contract schema at provided folder",
        )

    def __initialize_dataset_command(self):
        """This method initializes the dataset object generation command"""

        dataset_parser = self.sub_parser.add_parser(
            prog="oframework blueprint generate dataset",
            name="dataset",
            help="generates the dataset data objects",
        )
        dataset_parser.add_argument(
            "-n",
            "--dataset_name",
            required=True,
            type=str,
            help="dataset object name for the ",
        )
        dataset_parser.add_argument(
            "-f",
            "--dataset_object_folder",
            default="./",
            help="when specified it creates the dataset object in specified folder",
        )
        dataset_parser.add_argument(
            "-t",
            "--dataset_object_type",
            choices=["schema", "table", "view"],
            default="schema",
            help="when specified it creates the dataset object in specified folder",
        )

    def __initialize_pipeline_command(self):
        """This method initializes the pipeline generation command"""

        contract_parser = self.sub_parser.add_parser(
            prog="oframework blueprint generate pipeline",
            name="pipeline",
            help="generates the pipeline object",
        )
        contract_parser.add_argument(
            "-f",
            "--pipeline_folder",
            default="./",
            help="when specified it saves the pipeline schema created at provided folder",
        )

    def generate_contract(
        self, contract_type: str, contract_name: str, contract_folder: str
    ):
        """
        This method creates the contract template in target folder

        Parameters
        -------------------
        contract_type: str
            contract type to be specified to created
        contract_name: str
            contract name specified create at the provided contract folder
        contract_folder: str
            target folder to be created
        """

        if contract_folder is None:
            contract_folder = os.getcwd()
        contract_folder = os.path.join(os.path.abspath(contract_folder), contract_name)
        contract_schema_file = os.path.join(contract_folder, f"{contract_name}.json")
        if os.path.exists(contract_folder):
            console.print(
                "[bold][red] Contract name folder already exists. Please provide different contract name [/red][/bold]"
            )
            sys.exit(-1)
        else:
            os.makedirs(contract_folder, mode=777, exist_ok=True)
        try:
            module = importlib.import_module("projectoneflow.framework.contract.config")
            contract_schema_cls = getattr(
                module, f"{contract_type.lower().capitalize()}ContractSchema"
            )
            contract_example_schema = contract_schema_cls.generate_schema_definition(
                contract_name
            )
            with open(contract_schema_file, "w") as f:
                f.write(json.dumps(contract_example_schema))
            console.print(
                f"[bold]Created the Contract schema definition file at location {contract_folder}...[/bold]"
            )
            folder_names = contract_schema_cls.get_folder_name()

            for folder in folder_names:
                sub_folder = os.path.join(contract_folder, folder)
                if not os.path.exists(sub_folder):
                    os.makedirs(sub_folder, mode=777, exist_ok=True)
            console.print(
                f"[bold]Created the required folder for specific contract {contract_name} at location {contract_folder}...[/bold]"
            )
        except Exception as e:
            console.print(
                f"[bold][red] Problem in generating the example schema for project, Failed due to below issue {e} [/red][/bold]"
            )

    def generate_dataset(
        self, dataset_name: str, dataset_type: str, dataset_folder: str
    ):
        """
        This method creates the contract template in target folder

        Parameters
        -------------------
        contract_type: str
            contract type to be specified to created
        dataset_name: str
            dataset name specified create at the provided dataset folder
        dataset_type: str
            create a specific dataset object type at specific dataset folder
        dataset_folder: str
            target folder to be created
        """

        if dataset_folder is None:
            dataset_folder = os.getcwd()
        if dataset_type == "schema":
            dataset_folder = os.path.join(os.path.abspath(dataset_folder), dataset_name)
        dataset_schema_file = os.path.join(dataset_folder, f"{dataset_name}.json")
        if os.path.exists(dataset_folder):
            if dataset_type == "schema":
                console.print(
                    f"[bold][red] Dataset {dataset_type} name folder already exists. Please check the {dataset_type} name provided [/red][/bold]"
                )
                sys.exit(-1)
            if os.path.exists(dataset_schema_file):
                console.print(
                    f"[bold][red] Dataset {dataset_type} schema definition file already exists. Please check the {dataset_type} name provided [/red][/bold]"
                )
                sys.exit(-1)
        else:
            os.makedirs(dataset_folder, mode=777, exist_ok=True)
        try:
            module = importlib.import_module("projectoneflow.framework.contract.config")
            dataset_schema_cls = getattr(
                module, f"{dataset_type.lower().capitalize()}ObjectSchema"
            )
            dataset_example_schema = dataset_schema_cls.generate_schema_definition(
                dataset_name
            )
            with open(dataset_schema_file, "w") as f:
                f.write(json.dumps(dataset_example_schema))
            console.print(
                f"[bold]Created the dataset {dataset_type} object definition file at location {dataset_folder}...[/bold]"
            )
            if hasattr(dataset_schema_cls, "get_folder_name"):
                folder_names = dataset_schema_cls.get_folder_name()

                for folder in folder_names:
                    sub_folder = os.path.join(dataset_folder, folder)
                    if not os.path.exists(sub_folder):
                        os.makedirs(sub_folder, mode=777, exist_ok=True)
                console.print(
                    f"[bold]Created the required folder for dataset {dataset_type} object {dataset_name} at location {dataset_folder}...[/bold]"
                )
        except Exception as e:
            console.print(
                f"[bold][red] Problem in generating the example {dataset_type} object definition for {dataset_name}, Failed due to below issue {e} [/red][/bold]"
            )

    @staticmethod
    def input_from_choices(msg, choices):
        """
        This method asks for user input and check to compare choices

        Parameters
        --------------
        msg: str
            user prompt to be viewed by user
        choices: list
            choices to compare aganist

        Returns
        ---------------
        str
            returns the input which satisfied the choice
        """
        while True:
            value = input(
                f"{colorlog.escape_codes.escape_codes['bold_blue']}{msg}{colorlog.escape_codes.escape_codes['reset']}"
            )
            if value not in choices:
                console.print(
                    f"[bold][red]Please Enter Valid choice from {choices}[/red][/bold]"
                )
            else:
                return value

    @staticmethod
    def python_template(
        fn_name: str,
        fn_args: List[str],
        fn_body: List[str],
        fn_return_type: str,
        fn_import_st: str = None,
    ):
        """
        This method is used to create the python template

        Parameters
        -----------------
        fn_name: str
            function name
        fn_args: List[str]
            list of all possible function arguments
        fn_body: List[str]
            function body
        fn_return_type: str
            function return type
        fn_import_st: str
            function file import statement
        """
        function_str = []
        if fn_import_st is not None:
            function_str.extend(fn_import_st)
        function_str.append(f"def {fn_name}({','.join(fn_args)})->{fn_return_type}:")
        function_str.extend(["\t" + i for i in fn_body])

        final_str = "\n".join(function_str)
        return final_str

    @staticmethod
    def input(msg, reg_expr=None, reg_expr_msg=None):
        """
        This method asks for user input and checks for not None, empty

        Parameters
        --------------
        msg: str
            user prompt to be viewed by user


        Returns
        ---------------
        str
            returns the input which satisfied the choice
        """
        while True:
            value = input(
                f"{colorlog.escape_codes.escape_codes['bold_blue']}{msg}{colorlog.escape_codes.escape_codes['reset']}"
            )
            if len(value) == 1 and value == "" or value == " ":
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Please Enter Valid value \n{colorlog.escape_codes.escape_codes['reset']}"
                )
            elif (reg_expr is not None) and re.match(reg_expr, value) is None:
                sys.stderr.write(
                    f"{colorlog.escape_codes.escape_codes['bold_red']}Please Enter Valid value,{reg_expr_msg}  \n{colorlog.escape_codes.escape_codes['reset']}"
                )
            else:
                return value

    def resolve_spark(self, pipeline_name, target_directory):
        """
        Executes the Blueprint Databricks pipeline generation

        Parameters
        ----------------
        pipeline_name:str
            pipeline name to be resolved to be created
        target_directory:str
            target directory to where the pipeline template is saved into
        """
        from projectoneflow.core.schemas.deploy import PipelineTaskTypes, PipelineCluster
        from projectoneflow.core.schemas.sources import (
            SparkSource,
            SparkSourceExtractType,
            SparkSourceType,
            Sink,
            SinkType,
            WriteType,
        )
        from projectoneflow.core.sources import SourceProxy
        from projectoneflow.core.schemas.execution import SparkExecutionTypes

        target_directory = "./" if target_directory is None else target_directory
        tobe_dir = os.path.join(os.path.abspath(target_directory), pipeline_name)

        if os.path.exists(tobe_dir):
            console.print(
                f"[bold][red]Provided pipeline folder already exists. Please check the pipeline name provided [/red][/bold]"
            )
            sys.exit(-1)
        else:
            os.makedirs(tobe_dir, mode=777, exist_ok=True)

        # create the docs folder
        # docs_dir = os.path.join(tobe_dir, "docs")
        # if not os.path.exists(docs_dir):
        #     os.makedirs(docs_dir, mode=777, exist_ok=True)

        # docs_template = """### List of systems / list of databases or API end-points we are hitting\n### List of objects (database tables/ API entities / etc) that we are scoping to be pushed into Databricks ?\n### Stakeholders that we have consulted\n### Data Team Involved\n### Ingestion Details"""
        # with open(os.path.join(docs_dir, f"{pipeline_name}.md"), "w") as f:
        #     f.write(docs_template)

        # create the tests folder
        tests_dir = os.path.join(tobe_dir, "tests")
        if not os.path.exists(tests_dir):
            os.makedirs(tests_dir, mode=777, exist_ok=True)

        # create the execution folder
        execution_dir = os.path.join(tobe_dir, "execution")
        if not os.path.exists(execution_dir):
            os.makedirs(execution_dir, mode=777, exist_ok=True)

        # pipeline json creation
        pipeline_json = {"name": "pipeline_name"}
        pipeline_json["description"] = self.__class__.input(
            "Please enter the pipeline description: "
        )
        pipeline_json["refresh_policy"] = {
            "cron_expression": self.__class__.input(
                "When should you want to schedule the pipeline, Please enter the cron_expression: "
            )
        }
        pipeline_json["clusters"] = {}
        cluster_template = {
            k: v.default for k, v in PipelineCluster.model_fields.items()
        }
        while True:
            task_builder_json = {}
            task_builder_json["name"] = self.__class__.input(
                f"Please enter task name (or type exit to stop): "
            )
            if task_builder_json["name"] == "exit":
                break
            task_builder_json["type"] = self.__class__.input_from_choices(
                f"Please enter the pipeline task type, supported types {[PipelineTaskTypes.spark_task.value]}: ",
                choices=[PipelineTaskTypes.spark_task.value],
            )
            task_builder_json["description"] = self.__class__.input(
                "Please enter the task description: "
            )
            task_builder_json["cluster"] = self.__class__.input(
                "Please enter the cluster name to run this task: "
            )
            pipeline_json["clusters"][task_builder_json["cluster"]] = cluster_template
            task_builder_json["refresh_policy"] = {"type": "incremental"}
            # task input prompt block
            task_builder_json["input"] = []
            while True:
                input_builder_json = {}
                input_builder_json["name"] = self.__class__.input(
                    f"Please task input name (or exit to stop): "
                )
                if input_builder_json["name"] == "exit":
                    if len(task_builder_json["input"]) == 0:
                        console.print(
                            f"[bold][red]Task should have input, please start entering the input details[/red][/bold]"
                        )
                        continue
                    else:
                        break
                input_builder_json["source"] = self.__class__.input_from_choices(
                    f"Please enter the input source, supported types {SparkSource.to_list()}: ",
                    choices=SparkSource.to_list(),
                )
                input_builder_json["source_type"] = self.__class__.input_from_choices(
                    f"Please enter the input source type, supported types {SparkSourceType.to_list()}: ",
                    choices=SparkSourceType.to_list(),
                )
                input_builder_json["path"] = self.__class__.input(
                    f"Please enter the source location/table to read the data from: "
                )
                if input_builder_json["source_type"] == "stream":
                    task_builder_json["refresh_policy"]["type"] = "stream"
                input_builder_json["source_extract_type"] = (
                    self.__class__.input_from_choices(
                        f"How you want to read the source, supported types {SparkSourceExtractType.to_list()}: ",
                        choices=SparkSourceExtractType.to_list(),
                    )
                )
                input_builder_json["options"] = {
                    k: ""
                    for k, v in getattr(
                        getattr(
                            SourceProxy.get_source_class(input_builder_json["source"]),
                            "ReadOptions",
                        ),
                        "model_fields",
                    ).items()
                    if v.is_required
                }

                task_builder_json["input"].append(input_builder_json)

            # task execution prompt block
            task_builder_json["execution"] = {}
            task_builder_json["execution"]["type"] = SparkExecutionTypes.module.value
            execution_fn_name = self.__class__.input(
                f"Enter the execution function name which is used to execute the transformation: "
            )
            execution_source = self.__class__.input(
                f"Please enter the module name to find the execution function it should starts with prefix <execution.>: ",
                reg_expr=r"execution\.[a-zA-Z\.\_0-9]+",
                reg_expr_msg="Execution module should be in the format <execution.module_path",
            )
            task_builder_json["execution"]["source"] = execution_source
            task_builder_json["execution"]["name"] = execution_fn_name
            # task output prompt block
            task_builder_json["output"] = []
            while True:
                output_builder_json = {}
                output_builder_json["name"] = self.__class__.input(
                    f"Please task output name (or exit to stop): "
                )
                if output_builder_json["name"] == "exit":
                    if len(task_builder_json["output"]) == 0:
                        console.print(
                            f"[bold][red]Task should have input, please start entering the input details[/red][/bold]"
                        )
                        continue
                    else:
                        break
                output_builder_json["path"] = self.__class__.input(
                    f"Please enter the target location/table to write the data to : "
                )
                output_builder_json["sink"] = self.__class__.input_from_choices(
                    f"Please enter the output sink name, supported types {Sink.to_list()}: ",
                    choices=Sink.to_list(),
                )
                output_builder_json["sink_type"] = self.__class__.input_from_choices(
                    f"Please enter the output sink type, supported types {SinkType.to_list()}: ",
                    choices=SinkType.to_list(),
                )
                output_builder_json["write_type"] = self.__class__.input_from_choices(
                    f"How you want to write to the sink, supported types {WriteType.to_list()}: ",
                    choices=WriteType.to_list(),
                )
                output_builder_json["options"] = {
                    k: ""
                    for k, v in getattr(
                        getattr(
                            SourceProxy.get_source_class(output_builder_json["sink"]),
                            "WriteOptions",
                        ),
                        "model_fields",
                    ).items()
                    if v.is_required
                }

                task_builder_json["output"].append(output_builder_json)
            pipeline_json[task_builder_json["name"]] = task_builder_json

            execution_path = re.findall(r"\w+", execution_source)

            if len(execution_path) > 0:
                execution_path = pathlib.Path(
                    os.path.join(tobe_dir, os.path.sep.join(execution_path) + ".py")
                )
                execution_dir = execution_path.resolve().parent.__str__()
                if not os.path.exists(execution_dir):
                    os.makedirs(execution_dir, mode=777, exist_ok=True)
                input_names = [
                    f'{i["name"]}:DataFrame' for i in task_builder_json["input"]
                ]
                execution_body_build = [
                    (
                        "result="
                        + "{"
                        + ",".join(
                            [
                                '"' + i["name"] + """":None"""
                                for i in task_builder_json["output"]
                            ]
                        )
                        + "}"
                        if len(task_builder_json["output"]) > 1
                        else "result=None"
                    ),
                    "return result",
                ]
                execution_return_type = (
                    "Dict[DataFrame]"
                    if len(task_builder_json["output"]) > 1
                    else "DataFrame"
                )
                execution_import_st = [
                    "from pyspark.sql import DataFrame",
                    "from typing import Dict",
                ]
                python_template = self.__class__.python_template(
                    fn_name=execution_fn_name,
                    fn_args=input_names,
                    fn_body=execution_body_build,
                    fn_return_type=execution_return_type,
                    fn_import_st=execution_import_st,
                )

                with open(execution_path.__str__(), "a+") as f:
                    f.write(python_template)

        with open(os.path.join(tobe_dir, f"{pipeline_name}.json"), "w") as f:
            f.write(json.dumps(pipeline_json, indent=4))

        console.print(
            f"[bold][green]Genererated the pipeline {pipeline_name} at location {tobe_dir}...[/green][/bold]"
        )

    def generate_pipeline(self, target_directory: str = "./"):
        """
        Executes the Blueprint pipeline generation

        Parameters
        ----------------
        target_directory:str
            target directory to where the pipeline template is saved into
        """

        pipeline_name = self.__class__.input(
            "Please enter the pipeline name, which is used to create the Pipeline folder: "
        )
        pipeline_type = self.__class__.input_from_choices(
            "Please enter the pipeline type, supported types (spark): ",
            choices=["spark"],
        )

        resolve_pipeline_type = getattr(self, "resolve_" + pipeline_type)

        resolve_pipeline_type(pipeline_name, target_directory)

    def execute(self, args):
        """
        This is the method which parses and executes the spark task method
        """
        if args.generate_command is None:
            self.parser.print_help()
        else:
            if args.generate_command == "contract":
                self.generate_contract(
                    args.contract_type, args.contract_name, args.contract_folder
                )

            elif args.generate_command == "dataset":
                self.generate_dataset(
                    args.dataset_name,
                    args.dataset_object_type,
                    args.dataset_object_folder,
                )

            elif args.generate_command == "pipeline":
                self.generate_pipeline(args.pipeline_folder)


class BlueprintCliGroup(CliGroup):
    def __init__(self, blueprint_parser: argparse.ArgumentParser):
        """
        This is initialization method for task cli group

        Parameters
        ----------------
        parser: argparse.ArgumentParser
            parent parser to which child parser will be initialized
        """

        self.parser = blueprint_parser
        self.sub_parser = blueprint_parser.add_subparsers(
            title="Blueprint Commands", dest="blueprint_command"
        )
        self.sub_command = {}
        self.__initialize_generate_command()

    def __initialize_generate_command(self):
        """This method generates the blueprint"""

        generate_parser = self.sub_parser.add_parser(
            prog="oframework blueprint generate",
            name="generate",
            usage="""
        oframework [global options] blueprint [global options] generate

        The available commands for executed are listed below.
        The sub-command needs to be given first, followed by
        workflow/functionality specific arguments
        """,
            help="generates the blueprint for specific resource",
        )
        self.sub_command["generate"] = GenerateTaskCliGroup(
            generate_parser=generate_parser
        )

    def execute(self, args):
        """
        This is the method which parses and executes the blueprint method
        """
        if args.blueprint_command is None:
            self.parser.print_help()
        else:
            command = self.sub_command[args.blueprint_command]
            command.execute(args)
