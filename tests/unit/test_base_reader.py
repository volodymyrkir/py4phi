"""Contains tests for the BaseReader class."""
from py4phi.base_reader import BaseReader


def test_base_reader():
    assert BaseReader.supported_file_types is None

