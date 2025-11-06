from typing import Protocol, runtime_checkable
import argparse
import sys


class CommandParser(argparse.ArgumentParser):
    def error(self, message):
        # self.print_usage()
        sys.stderr.write(
            f"To see all {self.prog} command help, Please run:\n\t{self.prog} -h \n"
        )
        self.exit(22, "%s: ERROR: %s\n" % (self.prog, message))


@runtime_checkable
class CliGroup(Protocol):
    def __init__(self, parser: argparse.ArgumentParser):
        """
        This is initialization method for specific cli group

        Parameters
        ----------------
        parser: argparse.ArgumentParser
            parent parser to which child parser will be initialized
        """

    def execute(self, *args, **kwargs):
        """Execution function to execute the arguments passed to command"""
