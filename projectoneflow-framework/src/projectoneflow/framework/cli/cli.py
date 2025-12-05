from projectoneflow.framework.cli.blueprint import BlueprintCliGroup
from projectoneflow.core.cli import CliGroup
from projectoneflow.core.cli import CommandParser
from projectoneflow.framework.ci.cli import CICliGroup
from projectoneflow.framework.contract.env import EnvTypes
from projectoneflow.framework.contract import ContractType
from projectoneflow.framework.validation import Run, ResultEnum
from projectoneflow.framework.runner.databricks import DatabricksRunner
from projectoneflow.framework.runner.local import LocalRunner
from projectoneflow.core.schemas.deploy import (
    InfraStateBackendType,
    InfraStateBackendConfig,
    PipelineTypes,
)
from projectoneflow.framework.contract.env import Environment, EnvironmentMode
from projectoneflow.framework.contract import Contract
from projectoneflow.framework.cli import CliOutput, CliOutTypes, FormattingTable
from projectoneflow.framework.contract.config import SelectObject
from projectoneflow.framework.exception.contract import DataObjectPatternMismatch
import projectoneflow.framework as framework
from rich import box
from rich.console import Console
import tempfile
import os
import uuid
import sys
from projectoneflow.core.utils import replace_special_symbols
from projectoneflow.framework.utils import delete_file_if_exists


console = Console()


class ProjectOneflowFrameworkCli:
    def __init__(self):
        self.sub_command = {}
        self.parser = CommandParser(
            prog="oframework",
            usage="""
        oframework [global options] <subcommand> <args>
        """,
            description="""The available commands for executed are listed below.
        The primary workflow/functionality command needs to be given first, followed by
        workflow/functionality specific arguments""",
        )
        self.sub_parsers = self.parser.add_subparsers(
            title="Main commands", dest="command"
        )
        self.parser.add_argument(
            "-v",
            "--version",
            action="version",
            version=framework.__version__,
            help="show the version",
        )
        self.parser.add_argument(
            "-c",
            "--contract_type",
            default="project",
            choices=ContractType.to_list(),
            help="show the version",
        )
        self.__initialize_blueprint_command()
        self.__initialize_validate_command()
        self.__initialize_deploy_command()
        self.__initialize_destroy_command()
        self.__initialize_ci_command()
        self.__initialize_run_command()

    def __initialize_blueprint_command(self):
        task_parser = self.sub_parsers.add_parser(
            prog="oframework blueprint",
            name="blueprint",
            usage="""
        oframework [global options] blueprint <args>

        The available commands for executed are listed below.
        The sub-command needs to be given first, followed by
        workflow/functionality specific arguments
        """,
            help="Creates the Pipeline template folder",
        )
        self.sub_command["blueprint"] = BlueprintCliGroup(task_parser)

    def __initialize_ci_command(self):
        task_parser = self.sub_parsers.add_parser(
            prog="oframework ci",
            name="ci",
            usage="""
        oframework [global options] ci <args>

        The available commands for executed are listed below.
        The sub-command needs to be given first, followed by
        workflow/functionality specific arguments
        """,
            help="This sub-command is used for framework ci-cd deployment steps",
        )
        self.sub_command["ci"] = CICliGroup(task_parser)

    def __initialize_validate_command(self):
        task_parser = self.sub_parsers.add_parser(
            prog="oframework validate",
            name="validate",
            usage="""
        oframework [global options] validate <args>

        The available commands for executed are listed below.
        The sub-command needs to be given first, followed by
        workflow/functionality specific arguments
        """,
            help="validates the provided project folder",
        )
        task_parser.add_argument(
            "-f",
            "--project_folder",
            required=True,
            help="when specified it runs the validation on the provided folder",
        )
        task_parser.add_argument(
            "-e",
            "--environment",
            default="local",
            choices=EnvTypes.to_list(),
            help="when specified it runs the validation on the provided folder",
        )
        task_parser.add_argument(
            "-s",
            "--select",
            action="extend",
            nargs="+",
            help="when specified it runs the validation on the only the selected resources,where resource supported are the tables, schemas, views, pipelines.\t Need to provide in the form resource=resource_name.\t Ex: pipelines=pipeline_test1",
        )
        task_parser.add_argument(
            "-o",
            "--only_fail",
            action="store_true",
            help="when specified only returns the the failure validation error",
        )
        task_parser.add_argument(
            "-j",
            "--json",
            action="store_true",
            help="when specified only returns result in Json format",
        )
        self.sub_command["validate"] = self.__class__.execute_validate_command

    def __initialize_deploy_command(self):
        task_parser = self.sub_parsers.add_parser(
            prog="oframework deploy",
            name="deploy",
            usage="""
        oframework [global options] deploy <args>

        The available commands for executed are listed below.
        The sub-command needs to be given first, followed by
        workflow/functionality specific arguments
        """,
            help="deploys the provided project folder",
        )
        task_parser.add_argument(
            "-f",
            "--project_folder",
            required=True,
            help="when specified it runs the validation on the provided folder and deploy to target location",
        )
        task_parser.add_argument(
            "-e",
            "--environment",
            default="local",
            choices=EnvTypes.to_list(),
            help="when specified it runs the deployment on the target location",
        )
        task_parser.add_argument(
            "-t",
            "--target_deploy_directory",
            default=None,
            help="when specified it creates the deploy artifacts directory in the provided location",
        )
        task_parser.add_argument(
            "-p",
            "--plan",
            action="store_true",
            help="when specified only runs until plan",
        )
        task_parser.add_argument(
            "-b",
            "--backend_config",
            action="extend",
            nargs="+",
            help="when specified it creates the backend configuration for the terraform",
        )
        task_parser.add_argument(
            "-s",
            "--select",
            action="extend",
            nargs="+",
            help="when specified it runs the validation on the only the selected resources,where resource supported are the tables, schemas, views, pipelines.\t Need to provide in the form resource=resource_name.\t Ex: pipelines=pipeline_test1",
        )
        task_parser.add_argument(
            "-j",
            "--json",
            action="store_true",
            help="when specified only returns result in Json format",
        )
        self.sub_command["deploy"] = self.__class__.execute_deploy_command

    def __initialize_destroy_command(self):
        task_parser = self.sub_parsers.add_parser(
            prog="oframework destroy",
            name="destroy",
            usage="""
        oframework [global options] destroy <args>

        The available commands for executed are listed below.
        The sub-command needs to be given first, followed by
        workflow/functionality specific arguments
        """,
            help="destroy the resource in the provided project folder",
        )
        task_parser.add_argument(
            "-f",
            "--project_folder",
            required=True,
            help="when specified it runs the validation on the provided folder and deploy to target location",
        )
        task_parser.add_argument(
            "-e",
            "--environment",
            default="local",
            choices=EnvTypes.to_list(),
            help="when specified it runs the deployment on the target location",
        )
        task_parser.add_argument(
            "-t",
            "--target_deploy_directory",
            default=None,
            help="when specified it creates the deploy artifacts directory in the provided location",
        )
        task_parser.add_argument(
            "-b",
            "--backend_config",
            action="extend",
            nargs="+",
            help="when specified it creates the backend configuration for the terraform",
        )
        task_parser.add_argument(
            "-p",
            "--plan",
            action="store_true",
            help="when specified only runs until plan",
        )
        task_parser.add_argument(
            "-s",
            "--select",
            action="extend",
            nargs="+",
            help="when specified it runs the validation on the only the selected resources,where resource supported are the tables, schemas, views, pipelines.\t Need to provide in the form resource=resource_name.\t Ex: pipelines=pipeline_test1",
        )
        task_parser.add_argument(
            "-j",
            "--json",
            action="store_true",
            help="when specified only returns result in Json format",
        )
        self.sub_command["destroy"] = self.__class__.execute_destroy_command

    def __initialize_run_command(self):
        task_parser = self.sub_parsers.add_parser(
            prog="oframework run",
            name="run",
            usage="""
        oframework [global options] run <args>

        The available commands for executed are listed below.
        The sub-command needs to be given first, followed by
        workflow/functionality specific arguments
        """,
            help="runs the specific pipeline in the provided project folder",
        )
        task_parser.add_argument(
            "-f",
            "--project_folder",
            required=True,
            help="when specified it runs the validation on the provided folder and deploy to target location",
        )
        task_parser.add_argument(
            "-e",
            "--environment",
            default="local",
            choices=EnvTypes.to_list(),
            help="when specified it runs the deployment on the target location",
        )
        task_parser.add_argument(
            "-p",
            "--pipeline_name",
            required=True,
            help="when specified it run the specific pipeline in target location",
        )
        task_parser.add_argument(
            "-r",
            "--cluster_id",
            default=None,
            type=str,
            help="when specified it run the specific pipeline tasks on specific cluster as specified",
        )
        task_parser.add_argument(
            "-t",
            "--pipeline_timeout",
            default=10,
            type=int,
            help="when specified it run the specific pipeline with timeout in minutes",
        )
        self.sub_command["run"] = self.__class__.execute_run_command

    @staticmethod
    def _print_table(run: Run, filter_result=[ResultEnum.failed], json=False):
        table = FormattingTable(box=box.ROUNDED, show_lines=True)
        table.add_column("Result", no_wrap=True)
        table.add_column("Object Type", no_wrap=True)
        table.add_column("Check", max_width=50, overflow="fold")
        table.add_column("Object", max_width=32)
        table.add_column("Description", max_width=100, overflow="fold")
        table.add_column("Details", max_width=100, overflow="fold")
        table.add_column("location", max_width=50, overflow="fold")
        for check in run.checks:
            if check.result in filter_result:
                table.add_row(
                    check.mark_up_result(),
                    check.object_type,
                    check.name,
                    check.object_name,
                    check.description,
                    check.details if check.result != ResultEnum.passed else None,
                    check.location,
                )
        if json:
            return table.to_json()
        else:
            console.print(table)

    @staticmethod
    def _result(
        run: Run, filter_result=[ResultEnum.failed, ResultEnum.error], json=False
    ):
        result = ProjectOneflowFrameworkCli._print_table(run, filter_result, json)
        if json:
            run_result = {
                "checks": len(run.checks),
                "duration": (run.timestamp_end - run.timestamp_start).total_seconds(),
                "check_result": result,
            }
            return CliOutput(
                type=CliOutTypes.output,
                message=(
                    "valid"
                    if (run.result not in [ResultEnum.failed, ResultEnum.error])
                    else "not-valid"
                ),
                result=run_result,
            ).to_json()
        else:
            if run.result not in [ResultEnum.failed, ResultEnum.error]:
                console.print(
                    f"\n[bold]✅ data contract is valid. Run {len(run.checks)} checks. Took {(run.timestamp_end - run.timestamp_start).total_seconds()} seconds.[/bold]\n"
                )
            else:
                console.print(
                    f"\n[bold]❌ data contract is invalid, Please follow the check constraints[/bold]\n"
                )

    @staticmethod
    def execute_validate_command(args):
        """This command executes the validates command"""
        select_object = SelectObject()
        environment = Environment()
        environment.OF_CURRENT_ENV = args.environment

        if (args.select is not None) and len(args.select) > 0:
            for s in args.select:
                try:
                    selection = s.split("=")
                    select_object.add(selection[0], selection[1])
                except DataObjectPatternMismatch as e:
                    exception = CliOutput(
                        type=CliOutTypes.exception,
                        message=f"""[bold][red]Provided selection {s} is invalid because of error: {e}[/bold][/red]""",
                    )
                    if args.json:
                        return exception.to_json()
                    else:
                        console.print(exception.message)
                        sys.exit(-1)
                except Exception:
                    exception = CliOutput(
                        type=CliOutTypes.exception,
                        message=f"""[bold][red]Provided selection {s} is invalid where format should be key=value and key choices are {list(SelectObject.model_fields.keys())}[/bold][/red]""",
                    )
                    if args.json:
                        return exception.to_json()
                    else:
                        console.print(exception.message)
                        sys.exit(-1)
        try:
            contract_class = Contract.get_contract(ContractType(args.contract_type))

            contract = contract_class(
                folder_location=args.project_folder,
                environment=EnvTypes(args.environment),
            )

            validation_run = contract.validate(select_object=select_object)

            filter_result = [ResultEnum(i) for i in ResultEnum.to_list()]

            if args.only_fail:
                filter_result = [ResultEnum.error, ResultEnum.failed]

            result = ProjectOneflowFrameworkCli._result(
                validation_run, filter_result=filter_result, json=args.json
            )
            if args.json:
                return result
        except Exception as e:
            exception = CliOutput(
                type=CliOutTypes.exception,
                message=f"[bold][red]Validation of Contract failed due to error, {e}[/bold][/red]",
            )
            if args.json:
                return exception.to_json()
            else:
                console.print(exception.message)
                sys.exit(-1)

    @staticmethod
    def execute_deploy_command(args):
        """This command executes the deploy command"""
        environment = Environment()
        environment.OF_MODE = EnvironmentMode.deploy
        environment.OF_CURRENT_ENV = args.environment
        if args.environment == EnvTypes.local.value:
            exception = CliOutput(
                type=CliOutTypes.exception,
                message=f"[bold][red]Can't deploy the project in target enviornment because deployment only supports in {[env for env in EnvTypes.to_list() if env!=EnvTypes.local.value]} environments[/bold][/red]",
            )
            if args.json:
                return exception.to_json()
            else:
                console.print(exception.message)
                sys.exit(-1)
        DEFAULT_STATE_FILE = os.path.join(
            tempfile.gettempdir(), ".projectoneflow", "terraform.tfstate"
        )
        deploy_backend_config = None
        backend_config = environment.OF_TF_BACKEND_CONFIG
        if args.backend_config is not None:
            for c in args.backend_config:
                config = c.split("=", 1)
                backend_config[config[0]] = "".join(config[1:])

        if "type" in backend_config:
            if backend_config["type"] not in InfraStateBackendType.to_list():
                exception = CliOutput(
                    type=CliOutTypes.exception,
                    message=f"[bold][red]For Deploy backend config types supported are {InfraStateBackendType.to_list()}, please check provided the backend config[/bold][/red]",
                )
                if args.json:
                    return exception.to_json()
                else:
                    console.print(exception.message)
                    sys.exit(-1)
        else:
            backend_config["type"] = "local"
            backend_config["path"] = backend_config.get("path", DEFAULT_STATE_FILE)

        deploy_backend_config = InfraStateBackendConfig(
            type=backend_config["type"],
            configuration={k: v for k, v in backend_config.items() if k != "type"},
        )

        backend_local_config = {
            "type": "local",
            "path": environment.OF_MODE_DEPLOY_TEMP_LOCAL_STATE_FILE,
        }
        deploy_local_backend_config = InfraStateBackendConfig(
            type=backend_local_config["type"],
            configuration={
                k: v for k, v in backend_local_config.items() if k != "type"
            },
        )

        select_object = SelectObject()
        if (args.select is not None) and len(args.select) > 0:
            for s in args.select:
                try:
                    selection = s.split("=")
                    select_object.add(selection[0], selection[1])
                except DataObjectPatternMismatch as e:
                    exception = CliOutput(
                        type=CliOutTypes.exception,
                        message=f"""[bold][red]Provided selection {s} is invalid because of error: {e}[/bold][/red]""",
                    )
                    if args.json:
                        return exception.to_json()
                    else:
                        console.print(exception.message)
                        sys.exit(-1)
                except Exception:
                    exception = CliOutput(
                        type=CliOutTypes.exception,
                        message=f"""[bold][red]Provided selection {s} is invalid where format should be key=value and key choices are {list(SelectObject.model_fields.keys())}[/bold][/red]""",
                    )
                    if args.json:
                        return exception.to_json()
                    else:
                        console.print(exception.message)
                        sys.exit(-1)

        try:

            contract_class = Contract.get_contract(ContractType(args.contract_type))

            contract = contract_class(
                folder_location=args.project_folder,
                environment=EnvTypes(args.environment),
            )

            validation_run = contract.validate()

        except Exception as e:
            exception = CliOutput(
                type=CliOutTypes.exception,
                message=f"[bold][red]Validation of Contract failed due to error, {e}[/bold][/red]",
            )
            if args.json:
                return exception.to_json()
            else:
                console.print(exception.message)
                sys.exit(-1)

        if validation_run.result not in [ResultEnum.passed, ResultEnum.warning]:
            result = ProjectOneflowFrameworkCli._result(validation_run, json=args.json)
            if args.json:
                return result
        else:
            deploy_strategy = contract.get_deploy_strategy()
            console.print("[bold]Deployment build Started...[/bold]")
            build_out, err = deploy_strategy.deploy_build(
                backend_config=deploy_local_backend_config,
                deploy_directory=args.target_deploy_directory,
                select_object=select_object,
            )
            if err is not None:
                exception = CliOutput(
                    type=CliOutTypes.exception,
                    message=f"[bold][red]Deployment failed at build stage because of the error {err} [/bold][/red]",
                )
                if args.json:
                    return exception.to_json()
                else:
                    console.print(exception.message)
                    sys.exit(-1)
            console.print("[bold]Deployment build successfully completed...[/bold]")
            console.print("[bold]Deployment initialization started...[/bold]")
            initialization_out, err = deploy_strategy.deploy_initialize(
                deploy_directory=build_out.deploy_dir,
                backend_config=deploy_backend_config,
                local_state_file_location=backend_local_config["path"],
                reconfigure=True,
            )
            if err is not None:
                exception = CliOutput(
                    type=CliOutTypes.exception,
                    message=f"[bold][red]Deployment failed at deployment initialization stage because of the error {err} [/bold][/red]",
                )
                if args.json:
                    return exception.to_json()
                else:
                    console.print(exception.message)
                    sys.exit(-1)

            console.print("[bold]Deployment initialization completed...[/bold]")
            console.print("[bold]Planning the deployment ...[/bold]")
            plan_out, err = deploy_strategy.deploy_plan(
                deploy_directory=build_out.deploy_dir,
                targets=build_out.targets,
                state_result=initialization_out.state_out,
                only_plan=args.plan,
            )
            if err is not None:
                exception = CliOutput(
                    type=CliOutTypes.exception,
                    message=f"[bold][red]Deployment failed at planning build stage because of the error {err} [/bold][/red]",
                )
                if args.json:
                    return exception.to_json()
                else:
                    console.print(exception.message)
                    sys.exit(-1)
            console.print(
                f"[bold]Changes applied for the plan: {plan_out.plan_file}[/bold]"
            )
            if not args.plan:
                console.print("[bold]Applying the build deployment artifacts...[/bold]")
                deploy_out, err = deploy_strategy.deploy_apply(
                    deploy_directory=build_out.deploy_dir,
                    targets=build_out.targets,
                    plan_file=plan_out.plan_file,
                    state_result=initialization_out.state_out,
                )
                if err is not None:
                    exception = CliOutput(
                        type=CliOutTypes.exception,
                        message=f"[bold][red]Deployment failed at appling stage because of the error {err} [/bold][/red]",
                    )
                    if args.json:
                        return exception.to_json()
                    else:
                        console.print(exception.message)
                        sys.exit(-1)

                console.print("[bold]Applying changes stage completed...[/bold]")

            ### cleaning up the temporary state file
            delete_file_if_exists(environment.OF_MODE_DEPLOY_TEMP_LOCAL_STATE_FILE)
            ###
            if not args.plan:
                console.print("[bold][green]Deployment Completed...[/bold][/green]")
            if args.json:
                result = CliOutput(
                    type=CliOutTypes.output,
                    message=(
                        "Planning Completed" if args.plan else "Deployment completed"
                    ),
                    result={"plan_json": plan_out.plan_json},
                )
                return result.to_json()

    @staticmethod
    def execute_destroy_command(args):
        """This command executes the destroy command"""

        environment = Environment()
        environment.OF_MODE = EnvironmentMode.deploy
        environment.OF_CURRENT_ENV = args.environment
        if args.environment == EnvTypes.local.value:
            exception = CliOutput(
                type=CliOutTypes.exception,
                message=f"[bold][red]Can't destroy resources in the project in target enviornment because deployment only supports in {[env for env in EnvTypes.to_list() if env!=EnvTypes.local.value]} environments[/bold][/red]",
            )
            if args.json:
                return exception.to_json()
            else:
                console.print(exception.message)
                sys.exit(-1)
        DEFAULT_STATE_FILE = os.path.join(
            tempfile.gettempdir(), ".projectoneflow", "terraform.tfstate"
        )
        deploy_backend_config = None
        backend_config = environment.OF_TF_BACKEND_CONFIG
        if args.backend_config is not None:
            for c in args.backend_config:
                config = c.split("=", 1)
                backend_config[config[0]] = "".join(config[1:])

        if "type" in backend_config:
            if backend_config["type"] not in InfraStateBackendType.to_list():
                exception = CliOutput(
                    type=CliOutTypes.exception,
                    message=f"[bold][red]For destroy state backend config types supported are {InfraStateBackendType.to_list()}, please check provided the backend config[/bold][/red]",
                )
                if args.json:
                    return exception.to_json()
                else:
                    console.print(exception.message)
                    sys.exit(-1)
        else:
            backend_config["type"] = "local"
            backend_config["path"] = backend_config.get("path", DEFAULT_STATE_FILE)

        deploy_backend_config = InfraStateBackendConfig(
            type=backend_config["type"],
            configuration={k: v for k, v in backend_config.items() if k != "type"},
        )

        backend_local_config = {
            "type": "local",
            "path": environment.OF_MODE_DEPLOY_TEMP_LOCAL_STATE_FILE,
        }
        deploy_local_backend_config = InfraStateBackendConfig(
            type=backend_local_config["type"],
            configuration={
                k: v for k, v in backend_local_config.items() if k != "type"
            },
        )

        select_object = SelectObject()
        if (args.select is not None) and len(args.select) > 0:
            for s in args.select:
                try:
                    selection = s.split("=")
                    select_object.add(selection[0], selection[1])
                except DataObjectPatternMismatch as e:
                    exception = CliOutput(
                        type=CliOutTypes.exception,
                        message=f"""[bold][red]Provided selection {s} is invalid because of error: {e}[/bold][/red]""",
                    )
                    if args.json:
                        return exception.to_json()
                    else:
                        console.print(exception.message)
                        sys.exit(-1)
                except Exception:
                    exception = CliOutput(
                        type=CliOutTypes.exception,
                        message=f"""[bold][red]Provided selection {s} is invalid where format should be key=value and key choices are {list(SelectObject.model_fields.keys())}[/bold][/red]""",
                    )
                    if args.json:
                        return exception.to_json()
                    else:
                        console.print(exception.message)
                        sys.exit(-1)

        try:

            contract_class = Contract.get_contract(ContractType(args.contract_type))

            contract = contract_class(
                folder_location=args.project_folder,
                environment=EnvTypes(args.environment),
            )

            validation_run = contract.validate()

        except Exception as e:
            exception = CliOutput(
                type=CliOutTypes.exception,
                message=f"[bold][red]Validation of Contract failed due to error, {e}[/bold][/red]",
            )
            if args.json:
                return exception.to_json()
            else:
                console.print(exception.message)
                sys.exit(-1)

        if validation_run.result not in [ResultEnum.passed, ResultEnum.warning]:
            result = ProjectOneflowFrameworkCli._result(validation_run, json=args.json)
            if args.json:
                return result
        else:
            deploy_strategy = contract.get_deploy_strategy()
            console.print("[bold]Destroy resources build Started...[/bold]")
            build_out, err = deploy_strategy.deploy_build(
                backend_config=deploy_local_backend_config,
                deploy_directory=args.target_deploy_directory,
                select_object=select_object,
                destroy=True,
            )
            if err is not None:
                exception = CliOutput(
                    type=CliOutTypes.exception,
                    message=f"[bold][red]Destroy resources failed at build stage because of the error {err} [/bold][/red]",
                )
                if args.json:
                    return exception.to_json()
                else:
                    console.print(exception.message)
                    sys.exit(-1)
            console.print(
                "[bold]Destroy resources build successfully completed...[/bold]"
            )
            console.print("[bold]Destroy resources initialization started...[/bold]")
            initialization_out, err = deploy_strategy.deploy_initialize(
                deploy_directory=build_out.deploy_dir,
                backend_config=deploy_backend_config,
                local_state_file_location=backend_local_config["path"],
                reconfigure=True,
            )
            if err is not None:
                exception = CliOutput(
                    type=CliOutTypes.exception,
                    message=f"[bold][red]Destroy resources failed at initialization stage because of the error {err} [/bold][/red]",
                )
                if args.json:
                    return exception.to_json()
                else:
                    console.print(exception.message)
                    sys.exit(-1)

            console.print(
                "[bold]Destroying resources initialization completed...[/bold]"
            )
            console.print("[bold]Planning the Destroying resources ...[/bold]")
            plan_out, err = deploy_strategy.deploy_plan(
                deploy_directory=build_out.deploy_dir,
                targets=build_out.targets,
                destroy=True,
                state_result=initialization_out.state_out,
                only_plan=args.plan,
            )
            if err is not None:
                exception = CliOutput(
                    type=CliOutTypes.exception,
                    message=f"[bold][red]Destroy resources failed at planning build stage because of the error {err} [/bold][/red]",
                )
                if args.json:
                    return exception.to_json()
                else:
                    console.print(exception.message)
                    sys.exit(-1)
            console.print(
                f"[bold]Changes applied for the plan: {plan_out.plan_file}[/bold]"
            )
            if not args.plan:
                console.print(
                    "[bold]Applying the build Destroy resources artifacts...[/bold]"
                )
                destroy_out, err = deploy_strategy.deploy_apply(
                    deploy_directory=build_out.deploy_dir,
                    targets=build_out.targets,
                    plan_file=plan_out.plan_file,
                    destroy=True,
                    state_result=initialization_out.state_out,
                )
                if err is not None:
                    exception = CliOutput(
                        type=CliOutTypes.exception,
                        message=f"[bold][red]Destroy resources failed at appling stage because of the error {err} [/bold][/red]",
                    )
                    if args.json:
                        return exception.to_json()
                    else:
                        console.print(exception.message)
                        sys.exit(-1)

                console.print("[bold]Applying changes stage completed...[/bold]")

            ### cleaning up the temporary state file
            delete_file_if_exists(environment.OF_MODE_DEPLOY_TEMP_LOCAL_STATE_FILE)
            ###
            if not args.plan:
                console.print(
                    "[bold][green]Removing the selected resource Completed...[/bold][/green]"
                )
            if args.json:
                result = CliOutput(
                    type=CliOutTypes.output,
                    message=(
                        "Planning for destroy resources Completed"
                        if args.plan
                        else "Destroying the selected resources completed"
                    ),
                    result={"plan_json": plan_out.plan_json},
                )
                return result.to_json()

    @staticmethod
    def execute_run_command(args):
        """This command executes the run command"""
        environment = Environment()

        select_object = SelectObject()
        select_object.add("pipelines", args.pipeline_name)
        run_id = uuid.uuid1().hex
        DEFAULT_STATE_FILE = os.path.join(
            tempfile.gettempdir(),
            ".projectoneflow",
            "run",
            run_id,
            "terraform.tfstate",
        )
        DEFAULT_BUILD_DIR = os.path.join(
            tempfile.gettempdir(),
            ".projectoneflow",
            "run",
            "build",
            run_id,
        )
        deploy_backend_config = None
        backend_config = {}
        backend_config["type"] = "local"
        backend_config["path"] = backend_config.get("path", DEFAULT_STATE_FILE)

        deploy_backend_config = InfraStateBackendConfig(
            type=backend_config["type"],
            configuration={k: v for k, v in backend_config.items() if k != "type"},
        )

        environment.OF_PRESETTING_NAME_PREFIX = (
            f"[{args.environment} projectoneflow_run:{run_id}]"
        )
        environment.OF_PRESETTING_TAGS = {"TEST_RUN": "true"}
        environment.OF_MODE_RUN_PIPELINE_STATE_PREFIX = (
            f"{tempfile.gettempdir()}/{run_id}"
        )
        environment.OF_MODE = EnvironmentMode.run
        environment.OF_MODE_RUN_PIPELINE_ID = run_id
        environment.OF_CURRENT_ENV = args.environment
        environment.OF_MODE_RUN_PIPELINE_CLUSTER_ID = args.cluster_id

        contract_class = Contract.get_contract(ContractType(args.contract_type))

        contract = contract_class(
            folder_location=args.project_folder, environment=EnvTypes(args.environment)
        )

        validation_run = contract.validate()

        if validation_run.result not in [ResultEnum.passed, ResultEnum.warning]:
            ProjectOneflowFrameworkCli._result(validation_run)
        else:
            if args.environment != EnvTypes.local.value:
                deploy_strategy = contract.get_deploy_strategy()
                console.print(
                    "[bold]Job is started to deploy at target server...[/bold]"
                )
                build_out, err = deploy_strategy.deploy_build(
                    backend_config=deploy_backend_config,
                    deploy_directory=DEFAULT_BUILD_DIR,
                    select_object=select_object,
                )
                if err is not None:
                    console.print(
                        f"[bold][red]Job Build Stage Failed because of the error {err} [/bold][/red]"
                    )
                    sys.exit(-1)
                if len(build_out.targets) == 0:
                    console.print(
                        f"[bold][red]Job Build Stage failed to find the selected target data assets, Please check deploy block or deploy enviornment variables or any problem in selected data asset [/bold][/red]"
                    )
                    sys.exit(-1)
                initialization_out, err = deploy_strategy.deploy_initialize(
                    deploy_directory=build_out.deploy_dir,
                    backend_config=deploy_backend_config,
                    local_state_file_location=backend_config["path"],
                    reconfigure=True,
                )
                if err is not None:
                    console.print(
                        f"[bold][red]Job Deployment initialization stage because of the error {err} [/bold][/red]"
                    )
                    sys.exit(-1)

                plan_out, err = deploy_strategy.deploy_plan(
                    deploy_directory=build_out.deploy_dir,
                    targets=build_out.targets,
                    state_result=initialization_out.state_out,
                )
                if err is not None:
                    console.print(
                        f"[bold][red]Job Deployment planning stage because of the error {err} [/bold][/red]"
                    )
                    sys.exit(-1)
                deploy_out, err = deploy_strategy.deploy_apply(
                    deploy_directory=build_out.deploy_dir,
                    targets=build_out.targets,
                    plan_file=plan_out.plan_file,
                    state_result=initialization_out.state_out,
                )
                if err is not None:
                    console.print(
                        f"[bold][red]Job Deployment Apply stage failed because of the error {err} [/bold][/red]"
                    )
                    sys.exit(-1)
                console.print("[bold]Job deployed at target server...[/bold]")
                try:
                    if (
                        contract.config.pipelines.pipelines[args.pipeline_name].type
                        == PipelineTypes.spark
                    ):
                        job_id = deploy_out.deploy_output[
                            replace_special_symbols(
                                f"{contract.config.pipelines.pipelines[args.pipeline_name].config.name}_job_id"
                            )
                        ]["value"]
                        runner = DatabricksRunner.build(
                            contract.config.deploy.databricks
                        )
                        console.print(
                            "[bold]Job started running at target server...[/bold]"
                        )

                        job_out = runner.run(
                            job_id=job_id, timeout=int(args.pipeline_timeout)
                        )
                        if (job_out is not None) and (job_out.error is not None):
                            if hasattr(job_out, "error_message") and callable(
                                getattr(job_out, "error_message")
                            ):
                                console.print(
                                    f"[bold][red]{job_out.error_message()}[/bold][/red]"
                                )
                            else:
                                console.print(
                                    f"[bold][red]Job {args.pipeline_name} with job_id:{job_id} failed with error {job_out.error}[/bold][/red]"
                                )

                        else:
                            console.print(
                                f"[bold][green]Job Succedded with details:{job_out}\n[/bold][/green]"
                            )
                except Exception as e:
                    console.print(
                        f"[bold][red]Tried to run the job {args.pipeline_name} using the databricks server failed with error {e} \n[/bold][/red]"
                    )
                console.print(
                    "[bold]started to destroy the deployed resources at target server...[/bold]"
                )
                plan_out, err = deploy_strategy.deploy_plan(
                    deploy_directory=build_out.deploy_dir,
                    targets=build_out.targets,
                    destroy=True,
                    state_result=initialization_out.state_out,
                )
                if err is not None:
                    console.print(
                        f"[bold][red]Job Destroying planning stage failed because of the error {err}\n. Please remove artifacts in target environment [/bold][/red]"
                    )
                    sys.exit(-1)
                deploy_out, err = deploy_strategy.deploy_apply(
                    deploy_directory=build_out.deploy_dir,
                    targets=build_out.targets,
                    plan_file=plan_out.plan_file,
                    destroy=True,
                    state_result=initialization_out.state_out,
                )
                if err is not None:
                    console.print(
                        f"[bold][red]Job Destroying Apply stage failed because of the error {err}\n.Please remove artifacts in target environment manually[/bold][/red]"
                    )
                    sys.exit(-1)
                console.print(
                    "[bold]Completed destroying the deployed resources at target server...[/bold]"
                )
            else:
                runner = LocalRunner()
                pipeline = contract.config.pipelines.pipelines[args.pipeline_name]
                console.print("[bold]Job started running locally...[/bold]")
                job_out = runner.run(pipeline=pipeline)
                if (job_out is not None) and (job_out.error is not None):
                    console.print(f"[bold][red]{job_out.error_message()}[/bold][/red]")
                    sys.exit(-1)
                else:
                    console.print(
                        f"[bold][green]Job Succedded with details:{job_out}\n[/bold][/green]"
                    )
                    sys.exit(0)

    def execute(self):
        """
        This is the method which parses and executes the projectoneflow framework method
        """
        args = self.parser.parse_args()
        if args.command is None:
            self.parser.print_help()
        else:
            command = self.sub_command[args.command]
            if hasattr(command, "__class__") and issubclass(
                command.__class__, CliGroup
            ):
                command.execute(args)
            elif callable(command):
                command(args)


def main():
    """This is the main"""
    data_flow_parser = ProjectOneflowFrameworkCli()
    data_flow_parser.execute()


if __name__ == "__main__":
    main()
