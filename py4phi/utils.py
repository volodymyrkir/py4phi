"""Utility functions for the py4phi package."""
import os
import shutil
from typing import TypeVar, Union
from pathlib import Path

from py4phi.logger_setup import logger

DataFrame = TypeVar("DataFrame")
PathOrStr = Union[Path, str]


def prepare_location(location: PathOrStr) -> None:
    """
    Prepare location for saving purposes.

    Careful! Deletes all files and folders under `location` if they exist.

    Args:
    ----
    location (PathOrStr): Path to be prepared.

    """
    shutil.rmtree(location, ignore_errors=True)
    os.makedirs(location, exist_ok=True)
    logger.debug(f'Successfully prepared save location: {location}.')
