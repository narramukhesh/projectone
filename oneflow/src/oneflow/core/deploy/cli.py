from oneflow.core.cli import CliGroup
import argparse
from oneflow.core.deploy.terraform.cli import TerraformDeployCliGroup


class DeployCliGroup(CliGroup):
    def __init__(self, pipeline_parser: argparse.ArgumentParser):
        """
        This is initialization method for pipeline cli group

        Parameters
        ----------------
        parser: argparse.ArgumentParser
            parent parser to which child parser will be initialized
        """

        self.parser = pipeline_parser
        self.sub_parser = pipeline_parser.add_subparsers(
            title="Deploy Commands", dest="deploy_command"
        )
        self.sub_command = {}
        self.__initialize_terraform_command()

    def __initialize_terraform_command(self):
        """This method initializes the terraform command"""

        terraform_parser = self.sub_parser.add_parser(
            prog="oneflow deploy terraform",
            name="terraform",
            usage="""oneflow [global options] deploy terraform <args>

        This commands executes the resiurces deployment using terraform execution in target environment
        """,
            help="Executes the resources deployment using terraform execution in target environment",
        )
        self.sub_command["terraform"] = TerraformDeployCliGroup(terraform_parser)

    def execute(self, args):
        """
        This is the method which parses and executes the spark task method
        """
        if args.deploy_command is None:
            self.parser.print_help()
        else:
            command = self.sub_command[args.deploy_command]
            command.execute(args)
