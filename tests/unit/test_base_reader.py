"""Contains tests for the BaseReader class."""
from py4phi.readers.base_readers.base_reader import BaseReader


def test_base_reader():
    assert len(BaseReader.supported_file_types) == 0

