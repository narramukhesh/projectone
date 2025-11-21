from datetime import datetime
from typing import Dict, Optional
from oneflow.core.schemas import DateFormatTypes
import json
import re
import zipfile
import os
import requests


class DateUtils:
    DEFAULT_START_TIME = datetime(2000, 1, 1)

    @staticmethod
    def get_time():
        return datetime.now()

    @staticmethod
    def get_datetime(date):
        return datetime.fromtimestamp(int(date))

    @staticmethod
    def to_timestamp(value):
        if isinstance(value, datetime):
            return int(value.timestamp())

    @staticmethod
    def parse_date_value(value, interval, format):
        """This is a static method which is not implemented"""

    @staticmethod
    def format_date_value(format, *args):
        return args

    @staticmethod
    def get_date_to_current_value(start_value, format):

        format_type = (
            getattr(DateFormatTypes, format)
            if not isinstance(format, DateFormatTypes)
            else format
        )

        start_value_formatted = (
            datetime.strptime(start_value, format_type.value)
            if start_value is not None
            else datetime.now().strftime(format_type.value)
        )
        end_value_formatted = datetime.now().strftime(format_type.value)
        return start_value_formatted, end_value_formatted


class NamespaceDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


def replace_special_symbols(string: str) -> str:
    return re.sub(r"[^\w]", "", string)


def read_json_file(file_path: str):
    """This function read the json file and returns json parse data"""
    result = None
    with open(file_path, "r") as f:
        result = json.load(f)
    return result


def create_parent_folder(path: str, file: bool = False):
    """This function create folder for the provided source file path"""
    path = os.path.abspath(path)
    if file:
        path = os.path.sep.join(path.split(os.path.sep)[:-1])

    if not os.path.exists(path):
        os.makedirs(path, mode=777, exist_ok=True)


def extract_zip_file(file_path: str, target_location: str):
    """This method extracts the zip files into folder"""
    with zipfile.ZipFile(file_path, "r") as f:
        f.extractall(target_location)


def is_table_path_like(path: str):
    """This function will help to determine if provided path location looks like path or not"""
    pattern = r"^(\w+\.)*(\w+)$"
    group = re.match(pattern, path)

    if group is not None and group[0] == path:
        return True
    return False


def is_file_path_like(path: str):
    """This function will help to determine if provided path location looks like path or not"""
    if "/" in path or "\\" in path:
        return True
    elif len(path.split("/")) == 1 or len(path.split("\\")) == 1:
        return True
    else:
        return False


def remove_folder(location: str):
    """
    Remove the location folder as specified

    Parameters
    --------------
    location:str
        location of the folder to remove from
    """
    import shutil

    shutil.rmtree(location, ignore_errors=True)


def post_webhook_api(url: str, message: Dict[str, str] | None):
    """
    This method will be used to post the message in the provided url
    """
    result = requests.post(url=url, json=message)
    result.raise_for_status()
    return result
