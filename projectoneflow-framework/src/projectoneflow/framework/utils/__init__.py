import sys
from pathlib import Path
import re
import os


def is_windows():
    return ("win32" == sys.platform) or ("cygwin" == sys.platform)


def is_windows_path(path):
    p = Path(path)
    # Check if the path is absolute and contains a drive letter on Windows
    return p.is_absolute() and p.drive != ""


def delete_file_if_exists(path):
    """This function will be used to delete the file if exists"""
    if os.path.exists(path=path):
        os.unlink(path=path)


def remove_color_codes(text):
    # Regular expression to match ANSI color codes
    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    return ansi_escape.sub("", text)
