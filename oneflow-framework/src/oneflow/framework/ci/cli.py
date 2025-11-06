from oneflow.core.cli import CommandParser, CliGroup
import argparse
from oneflow.framework.contract.env import EnvTypes
from oneflow.framework.ci.comment import CIPlatformType, get_ci_client
import os
import json
import sys
from rich.console import Console

console = Console()


class Args(dict):
    """This is dummy class definition for the specifying the args for the argparse command"""

    def __getattr__(self, key):
        return self[key]


class CICliGroup(CliGroup):
    def __init__(self, ci_cli_paraser: argparse.ArgumentParser):
        """
        This is initialization method for CI cli group

        Parameters
        ----------------
        parser: argparse.ArgumentParser
            parent parser to which child parser will be initialized
        """
        self.parser = ci_cli_paraser
        self.sub_parser = ci_cli_paraser.add_subparsers(
            title="CI subprocess", dest="ci_command"
        )
        self.sub_command = {}
        self.__initialize_validate_command()
        self.__initialize_release_command()
        self.__initialize_prerelease_command()

    def __initialize_validate_command(self):
        """This method initializes the validate command"""
        validate_parser = self.sub_parser.add_parser(
            prog="dframework ci validate",
            name="validate",
            help="validate the contract folder specified and publish to the corresponding to ci platform",
        )
        validate_parser.add_argument(
            "-f",
            "--project_folders",
            required=True,
            nargs="+",
            action="extend",
            help="when specified it runs the validation on the provided folders",
        )
        validate_parser.add_argument(
            "-e",
            "--environment",
            default="dev",
            choices=["dev", "uat", "prod"],
            help="when specified it runs the validation on the provided folder",
        )
        validate_parser.add_argument(
            "-p",
            "--project_id",
            required=True,
            help="project id of the target ci platform",
        )
        validate_parser.add_argument(
            "-r",
            "--mr_id",
            required=True,
            help="project merge request id of the target ci platform",
        )
        validate_parser.add_argument(
            "-t",
            "--private_token",
            required=True,
            help="project private token of the target ci platform to deploy comment",
        )
        validate_parser.add_argument(
            "-ci",
            "--ci_platform",
            default="gitlab",
            choices=CIPlatformType.to_list(),
            help="project id of the target ci platform",
        )
        self.sub_command["validate"] = self.__class__.execute_validate_command

    def __initialize_prerelease_command(self):
        """This method initializes the pre-release command"""
        prerelease_parser = self.sub_parser.add_parser(
            prog="dframework ci prerelease",
            name="prerelease",
            help="validates and get the plan for the contract folder specified and publish to the corresponding to ci platform",
        )
        prerelease_parser.add_argument(
            "-f",
            "--project_folders",
            required=True,
            nargs="+",
            action="extend",
            help="when specified it runs the validation on the provided folders",
        )
        prerelease_parser.add_argument(
            "-e",
            "--environment",
            default="dev",
            choices=["dev", "uat", "prod"],
            help="when specified it runs the validation on the provided folder",
        )
        prerelease_parser.add_argument(
            "-b",
            "--backend_config",
            action="extend",
            nargs="+",
            help="when specified it creates the backend configuration for the terraform",
        )
        prerelease_parser.add_argument(
            "-p",
            "--project_id",
            help="project id of the target ci platform",
        )
        prerelease_parser.add_argument(
            "-r",
            "--mr_id",
            help="project merge request id of the target ci platform",
        )
        prerelease_parser.add_argument(
            "-t",
            "--private_token",
            help="project private token of the target ci platform to deploy comment",
        )
        prerelease_parser.add_argument(
            "-ci",
            "--ci_platform",
            default="gitlab",
            choices=CIPlatformType.to_list(),
            help="project id of the target ci platform",
        )
        self.sub_command["prerelease"] = self.__class__.execute_prerelease_command

    def __initialize_release_command(self):
        """This method initializes the release command"""
        release_parser = self.sub_parser.add_parser(
            prog="dframework ci release",
            name="release",
            help="release the contract folder specified and publish to the target platform",
        )
        release_parser.add_argument(
            "-f",
            "--project_folders",
            required=True,
            nargs="+",
            action="extend",
            help="when specified it runs the validation on the provided folders",
        )
        release_parser.add_argument(
            "-e",
            "--environment",
            default="dev",
            choices=["dev", "uat", "prod"],
            help="when specified it runs the validation on the provided folder",
        )
        release_parser.add_argument(
            "-b",
            "--backend_config",
            action="extend",
            nargs="+",
            help="when specified it creates the backend configuration for the terraform",
        )
        self.sub_command["release"] = self.__class__.execute_release_command

    @staticmethod
    def execute_validate_command(args):
        """This command executes the validates command"""
        from oneflow.framework.cli.cli import DataFlowFrameworkCli

        try:
            if len(args.project_folders) > 0:
                comment_body = []
                for project in args.project_folders:
                    framework_cli = DataFlowFrameworkCli()
                    validate_args = Args()
                    validate_args["json"] = True
                    validate_args["select"] = None
                    validate_args["contract_type"] = "project"
                    validate_args["environment"] = args.environment
                    validate_args["only_fail"] = True
                    validate_args["project_folder"] = project
                    validate_result = framework_cli.execute_validate_command(
                        validate_args
                    )
                    project_folder = os.path.basename(project)
                    body = ""
                    if validate_result["type"] == "EXCEPTION":
                        body = f'❌ **{project_folder}** has invalid project contract, has exception while validating the contract {validate_result["message"]}'
                    if validate_result["message"] == "valid":
                        body = f"✅ **{project_folder}** has valid project contract"
                    elif validate_result["message"] == "not-valid":
                        checks = json.dumps(
                            {"items": validate_result["result"]["check_result"]}
                        )
                        body = f"❌ **{project_folder}** has invalid project contract, Please check the below validation results and modify project contract \n```json:table \n{checks}\n ```"
                    comment_body.append(body)
                body = "\n".join(comment_body)

                ci_client = get_ci_client(CIPlatformType(args.ci_platform))
                ci_client.post_mr_comment(
                    args.project_id, args.mr_id, body, private_token=args.private_token
                )
            else:
                console.print(f"⚠️ No project folders to validate")
                sys.exit(0)

        except Exception as e:
            console.print(
                f"❌ Failure in validation stage of the contract because of the issue {e}"
            )
            sys.exit(-1)

    @staticmethod
    def execute_prerelease_command(args):
        """This command executes the validates command"""
        from oneflow.framework.cli.cli import DataFlowFrameworkCli

        CHANGE_MAPPING = {
            "create": ":sparkles: **NEW OBJECT**",
            "update": ":pencil2: **UPDATE TO OBJECT**",
            "delete": ":bomb: **DELETING OBJECT**",
        }
        try:
            if len(args.project_folders) > 0:
                comment_body = []
                for project in args.project_folders:
                    framework_cli = DataFlowFrameworkCli()
                    project_folder = os.path.basename(project)
                    prerelease_args = Args()
                    prerelease_args["json"] = True
                    prerelease_args["select"] = None
                    prerelease_args["contract_type"] = "project"
                    prerelease_args["environment"] = args.environment
                    prerelease_args["target_deploy_directory"] = None
                    prerelease_args["plan"] = True
                    prerelease_args["project_folder"] = project
                    prerelease_args["backend_config"] = (
                        args.backend_config
                        + [f"key={project_folder}_terraform.tfstate:{args.environment}"]
                        if args.backend_config is not None
                        else args.backend_config
                    )

                    prerelease_result = framework_cli.execute_deploy_command(
                        prerelease_args
                    )

                    body = ""
                    if prerelease_result["type"] == "EXCEPTION":
                        body = f'❌ **{project_folder}** has invalid project contract, has exception while getting the deploy plan from the contract {prerelease_result["message"]}'
                    elif (
                        prerelease_result["message"] == "not-valid"
                        and prerelease_result["type"] == "OUTPUT"
                    ):
                        checks = json.dumps(prerelease_result["result"]["check_result"])
                        body = f"❌ **{project_folder}** has invalid project contract, Please check the below validation results and modify project contract \n{checks}\n ```"
                    elif (
                        prerelease_result["message"] == "Planning Completed"
                        and prerelease_result["type"] == "OUTPUT"
                    ):
                        body = f"✅ **{project_folder}** has valid project contract"
                        changes = prerelease_result["result"]["plan_json"]
                        if "resource_changes" in changes:
                            changes = [
                                i
                                for i in changes["resource_changes"]
                                if i["change"]["actions"] != ["no-op"]
                                and i["change"]["actions"] != ["read"]
                            ]
                            if len(changes) == 0:
                                body += ", No Changes"
                            else:
                                body += ", Below are the contract object changes:\n\n"
                                for change in changes:
                                    body += f'- {CHANGE_MAPPING.get(change["change"]["actions"][0],"")} {change["address"]}\n'

                    comment_body.append(body)
                body = "\n".join(comment_body)

                ci_client = get_ci_client(CIPlatformType(args.ci_platform))
                ci_client.post_mr_comment(
                    args.project_id, args.mr_id, body, private_token=args.private_token
                )
            else:
                console.print(f"⚠️ No project folders to validate")
                sys.exit(0)

        except Exception as e:
            console.print(
                f"❌ Failure in pre-release stage for the contract because of the issue {e}"
            )
            sys.exit(-1)

    @staticmethod
    def execute_release_command(args):
        """This command executes the validates command"""

        # There is a problem with circular dependency, so need to check it in future
        from oneflow.framework.cli.cli import DataFlowFrameworkCli

        try:
            if len(args.project_folders) > 0:
                for project in args.project_folders:
                    framework_cli = DataFlowFrameworkCli()
                    project_folder = os.path.basename(project)
                    release_args = Args()
                    release_args["json"] = True
                    release_args["select"] = None
                    release_args["contract_type"] = "project"
                    release_args["environment"] = args.environment
                    release_args["target_deploy_directory"] = None
                    release_args["plan"] = False
                    release_args["project_folder"] = project
                    release_args["backend_config"] = (
                        args.backend_config
                        + [f"key={project_folder}_terraform.tfstate:{args.environment}"]
                        if args.backend_config is not None
                        else args.backend_config
                    )
                    release_result = framework_cli.execute_deploy_command(release_args)
                    body = ""
                    if release_result["type"] == "EXCEPTION":
                        body = f'❌ Deployment failed for {project_folder} has exception while getting the deploy plan from the contract {release_result["message"]}'

                    elif (
                        release_result["message"] == "not-valid"
                        and release_result["type"] == "OUTPUT"
                    ):
                        checks = json.dumps(release_result["result"]["check_result"])
                        body = f"❌ {project_folder} has invalid project contract, Please check the below validation results and modify project contract \n{checks}\n ```"
                    elif (
                        release_result["message"] == "Deployment completed"
                        and release_result["type"] == "OUTPUT"
                    ):
                        body = f"✅ Deployment Completed for {project_folder}..."
                    console.print(body)

            else:
                console.print(f"⚠️ No project folders to be released")
                sys.exit(0)

        except Exception as e:
            console.print(
                f"❌ Failure in release stage of the contract because of the issue {e}"
            )
            sys.exit(-1)

    def execute(self, args):
        """
        This is the method which parses and executes the ci command
        """
        if args.ci_command is None:
            self.parser.print_help()
        else:
            command = self.sub_command[args.ci_command]
            command(args)
