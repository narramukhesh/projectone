import projectoneflow.core as core
from projectoneflow.core.task.cli import TaskCliGroup
from projectoneflow.core.deploy.cli import DeployCliGroup
from projectoneflow.core.cli import CommandParser


class ProjectOneflowCli:
    def __init__(self):
        self.sub_command = {}
        self.parser = CommandParser(
            prog="projectoneflow",
            usage="""
        projectoneflow [global options] <subcommand> <args>
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
            version=core.__version__,
            help="show the version",
        )

        self.__initialize_task_command()
        self.__initialize_deploy_command()

    def __initialize_task_command(self):
        task_parser = self.sub_parsers.add_parser(
            prog="projectoneflow task",
            name="task",
            usage="""
        projectoneflow [global options] task <args>

        The available commands for executed are listed below.
        The sub-command needs to be given first, followed by
        workflow/functionality specific arguments
        """,
            help="Execute the projectoneflow task in specific environment",
        )
        self.sub_command["task"] = TaskCliGroup(task_parser)

    def __initialize_deploy_command(self):
        pipeline_parser = self.sub_parsers.add_parser(
            prog="projectoneflow deploy",
            name="deploy",
            usage="""
        projectoneflow [global options] deploy <args>

        The available commands for executed are listed below.
        The sub-command needs to be given first, followed by
        workflow/functionality specific arguments
        """,
            help="Execute the projectoneflow deploy command where deloy the resources in specific environment",
        )
        self.sub_command["deploy"] = DeployCliGroup(pipeline_parser)

    def execute(self):
        """
        This is the method which parses and executes the spark task method
        """
        args = self.parser.parse_args()
        if args.command is None:
            self.parser.print_help()
        else:
            command = self.sub_command[args.command]
            command.execute(args)


def main():
    """This is the main"""
    one_flow_parser = ProjectOneflowCli()
    one_flow_parser.execute()


if __name__ == "__main__":
    main()
