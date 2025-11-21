"""
This module is used for the debugging the spark dataframe
"""

from typing import Any


class DataLogger:
    """This class is used to display/investigate the dataframe records which helpfull for debugging"""


def data_logger(pre_message: str, data: Any, message_id: str, post_message: str):
    "This is the entry function to log the data"
