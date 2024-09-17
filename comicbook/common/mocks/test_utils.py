import json
import os
from typing import List


def abs_path(caller_path, relative_path):
    """Takes the absolute path of the caller file (eg. __file__) and a relative path from the caller file to a specific file (eg. a test resource file)."""
    return os.path.normpath(os.path.join(os.path.dirname(caller_path), relative_path))