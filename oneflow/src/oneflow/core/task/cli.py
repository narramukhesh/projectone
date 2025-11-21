from oneflow.core.cli import CliGroup
import argparse
from oneflow.core.task.spark.cli import SparkTaskCliGroup
from oneflow.core.cli import CommandParser


class TaskCliGroup(CliGroup):
    def __init__(self, task_parser: argparse.ArgumentParser):
        """
        This is initialization method for task cli group

        Parameters
        ----------------
        parser: argparse.ArgumentParser
            parent parser to which child parser will be initialized
        """

        self.parser = task_parser

        self.parser.add_argument(
            "-t",
            "--task_type",
            type=str,
            choices=["spark"],
            default=None,
            help="execute the task type",
        )
        self.parser.add_argument(
            "-c",
            "--task_configuration",
            type=str,
            help="Json String/file to be passed to configure task and execute",
        )
        self.parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            help="when specified execute in debug mode, where logging will be more intuitive",
        )
        self.sub_parser = task_parser.add_subparsers(
            title="Task Commands", dest="task_command"
        )
        self.sub_command = {}
        self.__initialize_spark_command()

    def __initialize_spark_command(self):
        """This method initializes the spark command"""

        spark_parser = self.sub_parser.add_parser(
            prog="oneflow task spark",
            name="spark",
            usage="""oneflow [global options] task  spark <args>

        This commands executes the spark execution in local/databricks environment
        """,
            help="Executes the spark task in local/databricks environment",
        )
        self.sub_command["spark"] = SparkTaskCliGroup(spark_parser)

    def execute(self, args):
        """
        This is the method which parses and executes the spark task method
        """
        if args.task_type is not None:
            command = self.sub_command[args.task_type]
            setattr(args, f"{args.task_type}_command", "run")
            setattr(args, "task_config", args.task_configuration)
            setattr(args, "debug", args.verbose)
            command.execute(args)
        elif args.task_command is None:
            self.parser.print_help()
        else:
            command = self.sub_command[args.task_command]
            command.execute(args)


def main():
    """This is the main"""
    task_parser = CommandParser(
        prog="task",
        usage="""
        task [global options] <subcommand> <args>
        """,
        description="""The available commands for executed are listed below.
        The primary workflow/functionality command needs to be given first, followed by
        workflow/functionality specific arguments""",
    )
    task_parser_command = TaskCliGroup(task_parser=task_parser)
    args = task_parser.parse_args()
    task_parser_command.execute(args)


if __name__ == "__main__":
    main()
